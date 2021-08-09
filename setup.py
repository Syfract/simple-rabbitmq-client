from setuptools import setup, find_packages

setup(
    name="rabbitmq_client",
    version="0.3",
    packages=find_packages(),
    install_requires="pika==1.2.0"
)
