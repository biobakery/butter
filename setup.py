from setuptools import setup

setup(
    name='butter',
    version='0.0.1',
    description="Manage AnADAMA git repositories",
    zip_safe=False,
    classifiers=[
        "Development Status :: 1 - Pre-Alpha"
    ],
    packages=['butter'],
    install_requires=[
        "git-fat==0.5.0"
    ],
    entry_points={
        'console_scripts': [
            'butter = butter.cli:main'
        ]
    }
)
