import io
import re
import setuptools


with io.open("README.md", "rt", encoding="utf8") as f:
    long_description = f.read()

with io.open("df_io/__init__.py", "rt", encoding="utf8") as f:
    version = re.search(r"__version__ = \'(.*?)\'", f.read()).group(1)


setuptools.setup(
    name="df_io",
    version=version,
    author="NAGY, Attila",
    author_email="nagy.attila@gmail.com",
    description="Helpers for doing IO with Pandas DataFrames",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/Mikata-Project/df_io',
    packages=setuptools.find_packages(),
    install_requires=['numpy', 's3fs', 'zstandard', 'pandas'],
    classifiers=[
        "Programming Language :: Python",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
