from setuptools import setup, find_packages
import contessa

setup(
    name="contessa",
    version=contessa.__version__,
    description="Data-quality framework",
    long_description=open("README.rst", "r").read(),
    long_description_content_type="text/x-rst",
    author="Mario Hunka",
    author_email="mario.hunka@kiwi.com",
    url="https://github.com/kiwicom/contessa",
    packages=find_packages(),
    package_data={"contessa": ["alembic.ini", "alembic/*", "alembic/**/*"]},
    install_requires=[
        "sqlalchemy>=1.2",
        "psycopg2>=2.7",
        "pandas>=0.24.0",
        "jinja2>=2.10",
        "alembic>=0.8.10",
        "click>=7.0",
        "packaging>=19.2",
    ],
    tests_require=["pytest"],
    python_requires=">=3.6",
    entry_points={
        "console_scripts": ["contessa-migrate=contessa.migration_runner:main"]
    },
    classifiers=[
        "Operating System :: OS Independent",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development :: Quality Assurance",
        "Programming Language :: Python :: 3",
    ],
)
