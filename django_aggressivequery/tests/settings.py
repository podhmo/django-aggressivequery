DEBUG = True
SECRET_KEY = "test"
ALLOWED_HOSTS = ['*']
INSTALLED_APPS = [
    'django_aggressivequery.tests',
]
DATABASES = {"default": {
    "ENGINE": "django.db.backends.sqlite3",
    "NAME": ":memory:"
}}
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
    }
}
