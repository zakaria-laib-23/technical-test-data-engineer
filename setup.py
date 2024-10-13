from setuptools import setup, find_packages

# Automatically read the dependencies from requirements.txt
with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="my_project",  # Your project name
    version="0.1.0",  # Your project version
    packages=find_packages(where="src"),  # Finds all packages in 'src'
    package_dir={"": "src"},  # Set 'src' as the source directory
    install_requires=required,  # Automatically populated from requirements.txt
    python_requires=">=3.9",  # Specify the required Python version
)
