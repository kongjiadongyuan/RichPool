from setuptools import setup, find_packages

required_packages = [
    "textual", 
    "IPython"
]

setup(
    name="RichPool",
    version="0.0.1",
    install_requires=required_packages,
    packages=find_packages(),
    package_data={'rich_pool': ['*.tcss']},
    author="kongjiadongyuan",
    author_email="zhaggbl@outlook.com",
    description="A more powerful multiprocessing library",
    license="MIT",
)