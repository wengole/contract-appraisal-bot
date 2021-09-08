import asyncio
import decimal
import json
import logging
import math
from dataclasses import dataclass
from datetime import timedelta, timezone, datetime
from itertools import zip_longest
from typing import Any, Dict, Set, Optional, List, Union

import discord
import redis
import requests
from aiohttp import web
from aiohttp.abc import BaseRequest
from dataclasses_json import dataclass_json
from dhooks import Embed
from discord import Message, User
from discord.ext import commands
from dynaconf import settings
from esipy import EsiApp, EsiClient, EsiSecurity
from esipy.cache import RedisCache
from loguru import logger

millnames = ["", " k", " M", " B", " T"]


def millify(n):
    n = float(n)
    millidx = max(
        0,
        min(
            len(millnames) - 1, int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3))
        ),
    )

    return f"{n / 10 ** (3 * millidx):.1f}{millnames[millidx]}"


class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


logging.basicConfig(handlers=[InterceptHandler()], level=0)

bot = commands.Bot(command_prefix="$")
redis_client = redis.from_url(settings.REDIS_URL)
cache = RedisCache(redis_client)

esiapp = EsiApp(cache=cache).get_latest_swagger
esisecurity = EsiSecurity(
    redirect_uri=settings.ESI_CALLBACK,
    client_id=settings.ESI_CLIENT_ID,
    secret_key=settings.ESI_SECRET_KEY,
    headers={"User-Agent": settings.ESI_USER_AGENT},
)
esiclient = EsiClient(
    security=esisecurity,
    retry_requests=True,
    cache=cache,
    headers={"User-Agent": settings.ESI_USER_AGENT},
)
routes = web.RouteTableDef()


@dataclass_json
@dataclass
class Contract:
    price: float = 0.0
    value: float = 0.0
    profit: float = 0.0
    most_valuable: str = ""

    @property
    def profit_percent(self):
        return (self.profit / self.price) * 100.0

    def update(self, kv: Dict[str, Union[str, float]]):
        for key, value in kv.items():
            if not hasattr(self, key):
                raise AttributeError
            setattr(self, key, value)


async def process_page_contracts(contracts):
    contracts_of_interest = []
    for contract in contracts:
        if contract.date_expired.v < datetime.now(timezone.utc) + timedelta(hours=1):
            continue
        if contract.type != "item_exchange":
            continue
        contracts_of_interest.append(contract)
    return contracts_of_interest


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks"""
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


async def get_contracts_for_region_id(
    region_id: int = 10000002, page: Optional[int] = None
) -> Dict[int, Contract]:
    max_pages = 1
    max_value = redis_client.get(f"max_page_for_{region_id}")
    if max_value is not None:
        max_pages = json.loads(max_value)
    if page is None:
        page = 1
        value = redis_client.get(f"last_page_for_{region_id}")
        if value is not None:
            page = json.loads(value)
    if page > max_pages:
        page = 1
    redis_client.set(f"last_page_for_{region_id}", page, ex=settings.CACHE.pages)
    op = esiapp.op["get_contracts_public_region_id"](region_id=region_id, page=page)
    response = esiclient.request(op)
    max_pages = response.header["X-Pages"][0]
    redis_client.set(f"max_page_for_{region_id}", max_pages, ex=settings.CACHE.pages)
    all_contracts_raw = await process_page_contracts(response.data)
    all_contracts: Dict[int, Contract] = {
        x.contract_id: Contract(price=x.price) for x in all_contracts_raw
    }
    contracts_to_load = []
    for contract_id in all_contracts.keys():
        contract = redis_client.get(f"parsed_contract_{contract_id}")
        if contract:
            all_contracts[contract_id] = Contract.from_json(contract)
        else:
            contracts_to_load.append(contract_id)
    ops = [
        esiapp.op["get_contracts_public_items_contract_id"](contract_id=contract_id)
        for contract_id in contracts_to_load
    ]
    reqs_and_resps = esiclient.multi_request(ops)
    type_ids = set()
    for req, resp in reqs_and_resps:
        for item in resp.data:
            type_ids.add(item["type_id"])
    item_prices = await get_prices_for_typeids(type_ids)
    for req, response in reqs_and_resps:
        contract_id = int(req._p["path"]["contract_id"])
        if response.data is not None and response.status == 200:
            all_contracts[contract_id].update(
                await appraise_contract_items(response.data, item_prices)
            )
            all_contracts[contract_id].profit = (
                all_contracts[contract_id].value - all_contracts[contract_id].price
            )
            redis_client.set(
                f"parsed_contract_{contract_id}",
                all_contracts[contract_id].to_json(),
                ex=settings.CACHE.contract,
            )
        elif response.data is None:
            logger.warning("No response data for {}", contract_id)
            all_contracts.pop(contract_id)
        else:
            logger.warning(
                "Response for {} was {response.status}: {response.data}",
                contract_id,
                response=response,
            )
            all_contracts.pop(contract_id)
    return all_contracts


async def get_prices_for_typeids(type_ids: Set[int]) -> Dict[int, Dict[str, Any]]:
    ops = []
    price_lookup = {}
    item_prices = redis_client.get("item_prices")
    if item_prices is None:
        logger.debug("Item prices not yet saved")
        item_prices = {}
    else:
        logger.debug("Loaded cached item prices")
        item_prices = json.loads(item_prices)
        logger.debug(f"{[x for x in item_prices.keys()]}")
    for chunk in grouper(type_ids, 1000):
        chunk = [x for x in chunk if x is not None and str(x) not in item_prices.keys()]
        logger.debug(chunk)
        if not chunk:
            return item_prices
        ops.append(esiapp.op["post_universe_names"](ids=chunk))
    reqs_and_resps = esiclient.multi_request(ops)
    for req, response in reqs_and_resps:
        if response.data is not None and response.status == 200:
            resp = requests.post(
                "https://evepraisal.com/appraisal.json?market=jita&persist=no",
                data={"raw_textarea": "\n".join([x.name for x in response.data])},
            )
            price_lookup.update(
                {
                    x["typeID"]: x["prices"]["sell"]["percentile"]
                    for x in resp.json()["appraisal"]["items"]
                }
            )
            item_prices.update(
                {
                    x.id: {"name": x.name, "price": price_lookup.get(x.id, 0)}
                    for x in response.data
                }
            )
        elif response.data is None:
            logger.warning("No response data")
        else:
            logger.warning(
                "Response was {response.status}: {response.data}", response=response,
            )
    logger.debug("Saving item prices")
    redis_client.set("item_prices", json.dumps(item_prices), ex=settings.CACHE.prices)
    return item_prices


async def appraise_contract_items(
    items: List[Any], item_prices: Dict[int, Dict[str, Any]]
) -> Dict[str, Any]:
    most_valuable = ""
    most_valuable_value = 0.0
    value = 0.0
    for item in items:
        item_value = item_prices.get(item.get("type_id", 0), {}).get(
            "price", 0.0
        ) * item.get("quantity", 1)
        if item["is_included"] is False:
            item_value = -item_value
        value += item_value
        item_name = item_prices.get(item["type_id"], {}).get("name", "Unknown Item")
        if item_value > most_valuable_value:
            most_valuable = item_name
            most_valuable_value = value
    return {"value": value, "most_valuable": most_valuable}


def update_stored_tokens(
    discord_userid: str,
    access_token: str,
    refresh_token: str,
    expires_in: int,
    token_type: str,
    **kwargs,
):
    cache_key = f"esi_tokens_{discord_userid}"
    redis_client.set(
        cache_key,
        json.dumps(
            {
                "access_token": access_token,
                "expires_in": expires_in,
                "token_type": token_type,
                "refresh_token": refresh_token,
                "token_expiry": datetime.utcnow().timestamp() + expires_in,
            }
        ),
    )


def refresh_token_for_user(discord_userid: str):
    cache_key = f"esi_tokens_{discord_userid}"
    tokens = redis_client.get(cache_key)
    if tokens:
        tokens = json.loads(tokens)
        expiry = tokens.pop("token_expiry", 0)
        tokens["expires_in"] = int(expiry - datetime.utcnow().timestamp())
        if tokens["expires_in"] < 0:
            tokens["access_token"] = ""
    else:
        raise RuntimeError("Token for user not stored")
    esisecurity.update_token(tokens)
    esisecurity.refresh()
    update_stored_tokens(
        discord_userid=discord_userid,
        access_token=esisecurity.access_token,
        refresh_token=esisecurity.refresh_token,
        expires_in=int(esisecurity.token_expiry - datetime.utcnow().timestamp()),
        token_type="Bearer",
    )


@routes.get("/contract")
async def contract(request: BaseRequest):
    contract_id = request.query.get("contract_id")
    user_id = request.query.get("user_id")
    if contract_id is None or user_id is None:
        return web.HTTPBadRequest(reason="Contract ID or User ID was not provided")
    refresh_token_for_user(user_id)
    op = esiapp.op["post_ui_openwindow_contract"](contract_id=contract_id)
    response = esiclient.request(op)
    return web.Response(text="Contract opened in EVE")


@routes.get("/callback")
async def callback(request: BaseRequest):
    code = request.query.get("code")
    state = request.query.get("state")
    if code is None:
        return web.HTTPForbidden(reason="Token not found")
    text = f"Your Discord user is now associated with the character you selected."
    tokens = esisecurity.auth(code)
    update_stored_tokens(
        discord_userid=state,
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        expires_in=tokens["expires_in"],
        token_type="Bearer",
    )
    return web.Response(text=text)


@routes.get("/next")
async def next_page(request: BaseRequest):
    region_id = request.query.get("region_id")
    offset = request.query.get("offset")
    user_id = request.query.get("user_id")
    if region_id is None or offset is None or user_id is None:
        return web.HTTPBadRequest(reason=f"One of region_id, user_id or offset missing")
    region_id = int(region_id)
    offset = int(offset)
    user_id = int(user_id)
    profits = await filter_contracts(region_id=region_id)
    embed_dict = await generate_embed(
        user_id=user_id, profits=profits, region_id=region_id, offset=offset
    )
    user: User = bot.get_user(user_id)
    await user.send(content="", embed=discord.Embed.from_dict(embed_dict))
    return web.Response(
        text="You'll receive a message on discord, please close this window."
    )


@bot.command()
async def login(ctx):
    await ctx.author.send(
        f"{esisecurity.get_auth_uri(ctx.author.id, scopes=['esi-ui.open_window.v1'])}"
    )


@bot.command()
async def top(ctx, region_name: str = "The Forge", min_profit_percent: str = "0.0"):
    try:
        refresh_token_for_user(ctx.author.id)
    except RuntimeError:
        await ctx.author.send("You need to login first, use `$login`")
        return
    try:
        min_profit_percent = decimal.Decimal(min_profit_percent)
    except ValueError:
        await ctx.author.send(
            "Use `$top {region_name} [{min_profit_percent}]`. min_profit_percent needs to be a decimal number between 0 and 100"
        )
        return
    loading_msg: Message = await ctx.author.send(
        "Loading contracts, this may take a couple of minutes..."
    )
    char_name = esisecurity.verify()["name"]
    region_id = None
    if len(region_name) >= 3:
        response = esiclient.request(
            esiapp.op["get_search"](
                categories=["region"], search=region_name, strict=False
            )
        )
        if response.status == 200 and response.data:
            region_id = getattr(response.data, "region", None)
            if region_id:
                region_id = region_id[0]
    await get_region_name_for_id(region_id)
    profits = await filter_contracts(region_id, min_profit_percent)
    await ctx.author.send(f"Logged in as {char_name}")
    embed_dict = await generate_embed(ctx.author.id, profits, region_id)
    await loading_msg.delete()
    await ctx.author.send(content="", embed=discord.Embed.from_dict(embed_dict))


async def get_region_name_for_id(region_id: int) -> Optional[str]:
    if region_id is None:
        return
    response = esiclient.request(
        esiapp.op["get_universe_regions_region_id"](region_id=region_id)
    )
    if response.status == 200 and response.data:
        return getattr(response.data, "name", None)
    return


async def filter_contracts(region_id: int, min_profit_percent: decimal.Decimal):
    all_contracts = await get_contracts_for_region_id(region_id=region_id)
    profits = {
        k: v
        for k, v in sorted(
            all_contracts.items(), key=lambda item: item[1].profit, reverse=True
        )
        if "Blueprint" not in v.most_valuable and "Container" not in v.most_valuable
    }
    return profits


async def generate_embed(user_id, profits, region_id, offset: int = 0):
    region_name = await get_region_name_for_id(region_id)
    embed = Embed(
        description=(
            f"{offset + 1} - {offset + settings.PER_PAGE} (of {len(profits)}) most profitable "
            f"contracts in {region_name}"
        ),
        color=0x03FC73,
        timestamp="now",
    )
    embed.set_author(name="Contract Appraisal Bot",)
    for contract_id, contract in {
        k: profits[k] for k in list(profits)[offset : offset + settings.PER_PAGE]
    }.items():
        name = f"{contract.most_valuable}" or "Unknown Item"
        value = (
            f" * Price: {millify(contract['price'])}\n"
            f" * Value: {millify(contract['value'])}\n"
            f" * Profit: {millify(contract['profit'])}\n"
            f"[Open Contract in game]({settings.BASE_URL}/contract?contract_id={contract_id}&user_id={user_id})"
        )
        embed.add_field(
            name=name, value=value,
        )
    embed.add_field(
        name=f"Next {settings.PER_PAGE}",
        value=f"[Click for more]({settings.BASE_URL}/next?region_id={region_id}&offset={offset + settings.PER_PAGE}&user_id={user_id})",
    )
    embed_dict = embed.to_dict()
    return embed_dict


if __name__ == "__main__":
    logger.info("Starting..")
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, port=settings.PORT)
        loop.run_until_complete(
            asyncio.gather(bot.start(settings.DISCORD_BOT_TOKEN), site.start())
        )
    except KeyboardInterrupt:
        loop.run_until_complete(asyncio.gather(bot.logout(), runner.cleanup()))
        # cancel all tasks lingering
    finally:
        loop.close()
