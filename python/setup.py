from setuptools import setup

setup(
    name='redis-rpc',
    packages=['redis_rpc'],
    version='0.1',
    description='Minimalistic rpc-over-redis library',
    author='Marcin Kaszynski',
    author_email='marcin.kaszynski@gmail.com',
    url='https://github.com/marcinkaszynski/redis-rpc',
    setup_requires=['pytest-runner'],
    install_requires=['redis'],
    tests_require=['pytest', 'pytest-redis', 'pytest-timeout', 'yapf'],
    python_requires=">=3.5")
