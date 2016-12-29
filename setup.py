from setuptools import setup, find_packages


setup(
    name='cronosparser',
    version='0.2',
    description="Parser for CronosPro / CronosPlus database files.",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ],
    keywords='files walk index survey',
    author='OCCRP',
    author_email='tech@occrp.org',
    url='http://github.com/occrp/cronosparser',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    package_data={},
    include_package_data=True,
    zip_safe=False,
    test_suite='nose.collector',
    install_requires=[
        'six',
        'click',
        'unicodecsv',
        'normality'
    ],
    tests_require=[
        'nose',
        'coverage',
    ],
    entry_points={
        'console_scripts': [
            'cronos2csv = cronos.cli:main'
        ]
    }
)
