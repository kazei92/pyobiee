import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyobiee", # Replace with your own username
    version="0.2.0",
    author="Timur Kazyev",
    author_email="kazyev@me.com",
    description="Python wrapper for OBIEE",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kazei92/pyobiee",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
)
