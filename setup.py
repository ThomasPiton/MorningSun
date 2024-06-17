from setuptools import setup, find_packages

setup(
    name="MorningSun",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="MorningSun is a cool tool for grabbing financial data from Morningstar.",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/morning_sun",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "requests>=2.25.1",
        "beautifulsoup4>=4.9.3",
        "pandas>=1.2.0",
        # Add other dependencies here
    ],
    entry_points={
        'console_scripts': [
            'morning_sun=morning_sun.main:main',
        ],
    },
    include_package_data=True,
    package_data={
        # If any package contains *.txt or *.md files, include them:
        '': ['*.txt', '*.md'],
    },
)