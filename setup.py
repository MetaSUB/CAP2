
import setuptools

setuptools
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
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
    ],
    package_data={'capalyzer': [
        'packet_parser/ncbi_tree/*.dmp.gz',
        'packet_parser/microbe-directory.csv',
    ]},
)