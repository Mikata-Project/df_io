import io
import setuptools
import df_io


with io.open("README.md", "rt", encoding="utf8") as f:
    long_description = f.read()


setuptools.setup(
    name="df_io",
    version=df_io.__version__,
    author="NAGY, Attila",
    author_email="nagy.attila@gmail.com",
    description="Helpers for doing IO with Pandas DataFrames",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/Mikata-Project/df_io',
    packages=setuptools.find_packages(),
    install_requires=['numpy', 's3fs'],
    classifiers=[
        "Programming Language :: Python",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
