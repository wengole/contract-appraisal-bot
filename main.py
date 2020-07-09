import asyncio
import json
import logging
from datetime import timedelta, timezone, datetime
from itertools import zip_longest
from typing import Any, Dict, Set

import redis
import requests
from aiohttp import web
from aiohttp.abc import BaseRequest
from asgiref.sync import sync_to_async
from discord.ext import commands
from dynaconf import settings
from esipy import EsiApp, EsiClient, EsiSecurity
from esipy.cache import RedisCache
from loguru import logger


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


def process_page_contracts(contracts):
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


def get_contracts_for_region_id(region_id: int = 10000002) -> Dict[int, Any]:
    op = esiapp.op["get_contracts_public_region_id"](region_id=region_id)
    response = esiclient.request(op)
    pages = response.header["X-Pages"][0]
    all_contracts = process_page_contracts(response.data)
    ops = [
        esiapp.op["get_contracts_public_region_id"](region_id=region_id, page=page)
        for page in range(1, pages + 1)
    ]
    reqs_and_resps = esiclient.multi_request(ops)
    for req, response in reqs_and_resps:
        all_contracts.extend(process_page_contracts(response.data))
    all_contracts = {x.contract_id: x for x in all_contracts}
    contracts_to_load = []
    for contract_id in all_contracts.keys():
        contract = redis_client.get(f"parsed_contract_{contract_id}")
        if contract:
            all_contracts[contract_id] = json.loads(contract)
        else:
            contracts_to_load.append(contract_id)
    ops = [
        esiapp.op["get_contracts_public_items_contract_id"](contract_id=contract_id)
        for contract_id in contracts_to_load
    ]
    reqs_and_resps = esiclient.multi_request(ops)
    for req, response in reqs_and_resps:
        contract_id = int(req._p["path"]["contract_id"])
        if response.data is not None and response.status == 200:
            all_contracts[contract_id].items = response.data
            redis_client.set(
                f"parsed_contract_{contract_id}",
                json.dumps(all_contracts[contract_id], default=str),
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


def get_prices_for_typeids(type_ids: Set[int]) -> Dict[int, Dict[str, Any]]:
    ops = []
    price_lookup = {}
    item_prices = {}

    for chunk in grouper(type_ids, 1000):
        chunk = [x for x in chunk if x is not None]
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
    return item_prices


def appraise_contracts():
    contracts = get_contracts_for_region_id()
    type_ids = set()
    for contract in contracts.values():
        [type_ids.add(x["type_id"]) for x in contract["items"]]
    item_prices = get_prices_for_typeids(type_ids)
    for contract_id, contract in contracts.items():
        for item in contract.get("items", []):
            item["value"] = item_prices.get(item.get("type_id", 0), {}).get(
                "price", 0.0
            ) * item.get("quantity", 1)
            if item["is_included"] is False:
                item["value"] = -item["value"]
            item["name"] = item_prices.get(item["type_id"], {}).get(
                "name", "Unknown Item"
            )
        contract["value"] = sum([x["value"] for x in contract.get("items", []) if x])
        contract["profit"] = contract["value"] - contract["price"]
        redis_client.set(
            f"parsed_contract_{contract_id}", json.dumps(contract, default=str)
        )
    return contracts


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
    cache_key = f"parsed_contract_{contract_id}"
    contract = redis_client.get(cache_key)
    if contract is None:
        return web.HTTPNotFound(reason="Contract with that ID was not found")
    contract = json.loads(contract)
    op = esiapp.op["post_ui_openwindow_contract"](contract_id=contract["contract_id"])
    response = esiclient.request(op)
    return web.Response(text="Contract opened in EVE")


@routes.get("/callback")
async def callback(request: BaseRequest):
    code = request.query.get("code")
    state = request.query.get("state")
    if code is None:
        return web.HTTPForbidden(reason="Token not found")
    text = f"Hello, world {code}"
    tokens = esisecurity.auth(code)
    update_stored_tokens(
        discord_userid=state,
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        expires_in=tokens["expires_in"],
        token_type="Bearer",
    )
    return web.Response(text=text)


@bot.command()
async def login(ctx):
    await ctx.author.send(
        f"{esisecurity.get_auth_uri(ctx.author.id, scopes=['esi-ui.open_window.v1'])}"
    )


@bot.command()
async def top(ctx, region_name: str = "The Forge"):
    refresh_token_for_user(ctx.author.id)
    name = esisecurity.verify()["name"]
    all_contracts = await sync_to_async(appraise_contracts)()
    profits = sorted(
        filter(
            lambda x: all(
                y.get("is_blueprint_copy", False) is False for y in x["items"]
            )
            and not any(["Blueprint" in y["name"] for y in x["items"]])
            and x["profit"] > 100000000,
            all_contracts.values(),
        ),
        key=lambda x: x["profit"],
        reverse=True,
    )
    await ctx.author.send(f"Logged in as {name}")
    text = ""
    for contract in profits[:10]:
        text = f"{text}{settings.BASE_URL}/contract?contract_id={contract['contract_id']}&user_id={ctx.author.id}\n"
    await ctx.author.send(text)


if __name__ == "__main__":
    logger.info("Starting..")
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, "localhost", settings.PORT)
        loop.run_until_complete(
            asyncio.gather(bot.start(settings.DISCORD_BOT_TOKEN), site.start())
        )
    except KeyboardInterrupt:
        loop.run_until_complete(asyncio.gather(bot.logout(), runner.cleanup()))
        # cancel all tasks lingering
    finally:
        loop.close()
