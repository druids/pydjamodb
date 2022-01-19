from setuptools import setup, find_packages


setup(
    name='pydjamodb',
    version='0.0.9',
    description="Django interface to PyDjamoDB.",
    keywords='django, DynamoDB, PyDjamoDB',
    author='Lubos Matl',
    author_email='matllubos@gmail.com',
    url='https://github.com/druids/pydjamodb',
    license='MIT',
    package_dir={'pydjamodb': 'pydjamodb'},
    include_package_data=True,
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: Czech',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Internet :: WWW/HTTP :: Site Management',
    ],
    install_requires=[
        'django>=2.0, <4.0',
    ],
    zip_safe=False
)
