import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="s3mothball",
    version="0.0.1",
    author="Jack Cushman",
    author_email="jcushman@law.harvard.edu",
    description="Archival tool to prepare collections of small files on S3 for Glacier storage.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/harvard-lil/s3mothball",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3',
    install_requires=[
        "boto3",
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