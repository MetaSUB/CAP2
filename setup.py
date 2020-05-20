
import setuptools

setuptools.setup(
    name='cap2',
    version='0.1.0',
    description="CAP2",
    author="David C. Danko",
    author_email='dcdanko@gmail.com',
    url='https://github.com/metasub/cap2',
    packages=setuptools.find_packages(),
    package_dir={'cap2': 'cap2'},
    install_requires=[
        'pandas',
        'numpy',
        'luigi',
        'PyYaml',
        'biopython',
        'click',
    ],
    entry_points={
        'console_scripts': [
            'cap2=cap2.cli:main',
        ]
    },
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
    ],
)
