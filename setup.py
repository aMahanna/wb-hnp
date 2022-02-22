from setuptools import setup

setup(
    name="wb-hnb",
    version="0.0.0",
    description="A World Bank Health Nutrition Population Data Mart.",
    url="https://github.com/aMahanna/wb-hnp",
    python_requires=">=3.6",
    license="MIT License",
    packages=["src"],
    install_requires=[
        "psycopg2-binary==2.9.3",
        "python-dotenv==0.19.0",
    ],
)
