import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="s3mothball",
    version="0.0.1",
    # author="Example Author",
    # author_email="author@example.com",
    # description="A small example package",
    # long_description=long_description,
    # long_description_content_type="text/markdown",
    # url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    # classifiers=[
    #     "Programming Language :: Python :: 3",
    #     "License :: OSI Approved :: MIT License",
    #     "Operating System :: OS Independent",
    # ],
    python_requires='>=3.6',
    install_requires=[
        "boto3",
        "python-dateutil",
        "smart-open>=1.10.0",
        "tqdm",
    ],
    tests_require=[
        "pytest",
        "moto",
    ],
    setup_requires=[
        'pytest-runner',
    ],
    entry_points = {
        'console_scripts': [
            's3mothball=s3mothball.commands:main',
        ],
    }
)