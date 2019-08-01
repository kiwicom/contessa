from setuptools import setup, find_packages

setup(
    name="contessa",
    version="0.1",
    description="Data-quality framework",
    # long_description=get_long_description(),
    # long_description_content_type="text/markdown",
    author="mario.hunka",
    author_email="mario.hunka@kiwi.com",
    packages=find_packages(),
    include_package_data=True,
    install_requires=["sqlalchemy>=1.2", "psycopg2>=2.7"],
    tests_require=["pytest"],
    python_requires=">=3.6",
    classifiers=[
        "Private :: Do Not Upload",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
    ],
)
