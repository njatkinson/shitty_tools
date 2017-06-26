from distutils.core import setup

setup(
    name = 'shitty_tools',
    packages = ['shitty_tools', 'shitty_tools.key_value', 'shitty_tools.rpc'],
    version = '2017.06.26.0',
    description = 'A collection of Python modules including tools for concurrency, connection pools, key-value storage'
                  ' and rate limiting.',
    author = 'Nate Atkinson',
    url = 'https://github.com/njatkinson/shitty_tools',
    download_url = 'https://github.com/njatkinson/shitty_tools/archive/2017.06.26.0.tar.gz',
    keywords = ['connection pool', 'key value', 'rate limit', 'sqlalchemy']
)