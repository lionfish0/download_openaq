from distutils.core import setup
setup(
  name = 'download_openaq',
  packages = ['download_openaq'], # this must be the same as the name above
  version = '1.01',
  description = 'Download data from an openaq channel using their API',
  author = 'Mike Smith',
  author_email = 'm.t.smith@sheffield.ac.uk',
  url = 'https://github.com/lionfish0/download_openaq.git',
  keywords = ['openaq'],
  classifiers = [],
  install_requires=['py-openaq','pandas'],
)

