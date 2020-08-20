
import setuptools

setuptools.setup(
    name='cap2',
    version='0.1.1',
    description="CAP2",
    author="David C. Danko",
    author_email='dcdanko@gmail.com',
    url='https://github.com/metasub/cap2',
    packages=setuptools.find_packages(),
    package_dir={'cap2': 'cap2'},
    install_requires=[
        'pandas',
        'numpy',
        'python-daemon',
        'luigi==3.0.0b2',
        'PyYaml==5.3.1',
        'biopython==1.76',
        'click==6.7',
        'multiqc==1.8',  # hackish, tbd if I'm okay with this
        'humann2==2.8.2'
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
