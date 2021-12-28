
import setuptools

setuptools.setup(
    name='cap2',
    version='0.4.1',
    description="CAP2",
    author="David C. Danko",
    author_email='dcdanko@gmail.com',
    url='https://github.com/metasub/cap2',
    packages=setuptools.find_packages(),
    package_dir={'cap2': 'cap2'},
    install_requires=[
        'pandas',
        'numpy',
        'scipy',
        'libjpeg-dev',
        'python-daemon',
        'luigi==3.0.0b2',
        'PyYaml>=5.3.1',
        'pillow==8.4.0',
        'biopython==1.76',
        'click>=6.7',
        'Jinja2>=3.0.0a1',  # for multiqc
        'multiqc',  # hackish, tbd if I'm okay with this
        'pangea_api>=0.9.3',
        'pysam',
        'python-louvain',
        'gimmebio.seqs',
        'bloom_filter',
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
