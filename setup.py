from setuptools import setup, find_packages

setup(
    name="contessa",
    version="0.1.4",
    description="Data-quality framework",
    long_description=open("README.rst", "r").read(),
    long_description_content_type="text/markdown",
    author="Mario Hunka",
    author_email="mario.hunka@kiwi.com",
    url="https://github.com/kiwicom/contessa",
    packages=find_packages(),
    include_package_data=True,
    install_requires=["sqlalchemy>=1.2", "psycopg2>=2.7", "jinja2>=2.10.1"],
    tests_require=["pytest"],
    python_requires=">=3.6",
    classifiers=[
        "Operating System :: OS Independent",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development :: Quality Assurance",
        "Programming Language :: Python :: 3",
    ],
)
