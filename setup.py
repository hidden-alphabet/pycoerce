import setuptools

setuptools.setup(
    name='pycoerce',
    version='0.0.1',
    author=["Cole Hudosn"],
    author_email="cole@colejhudson.com",
    description="Encode python objects into other type systems (e.g. SQL, PyArrow, etc)",
    install_requires=["pyarrow"],
    packages=setuptools.find_packages()
)
