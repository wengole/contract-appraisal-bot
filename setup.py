from setuptools import setup

install_requires = [
    "aiohttp==3.6.2",
    "discord.py==1.3.3",
    "EsiPy==1.0.0",
    "dynaconf==2.2.3",
    "redis==3.5.3",
    "requests==2.24.0",
    "dhooks==1.1.3",
    "black==19.10b0",
    "loguru==0.5.1",
    "asgiref==3.2.10",
]

setup(
    name="contract-appraisal-bot",
    version="0.0.1",
    packages=[""],
    url="",
    license="",
    author="Ben Cole",
    author_email="",
    description="",
    install_requires=install_requires,
)
