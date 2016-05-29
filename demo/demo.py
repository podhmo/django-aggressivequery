# -*- coding:utf-8 -*-

"""
aggressive query
need: pip install django-aggressivequery
"""
import django
from django.db import models
from django.conf import settings
from django.db import connections


settings.configure(
    DEBUG=True,
    DATABASES={"default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:"
    }},
    INSTALLED_APPS=[__name__]
)
django.setup()


def create_table(model):
    connection = connections['default']
    with connection.schema_editor() as schema_editor:
        schema_editor.create_model(model)


class User(models.Model):
    name = models.CharField(max_length=255, default="", null=False)
    ctime = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = __name__
        db_table = "user"


class UserInfo(models.Model):
    point = models.IntegerField(null=False, default=0)
    user = models.OneToOneField(User, related_name="info")
    ctime = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = __name__
        db_table = "userinfo"


class Team(models.Model):
    users = models.ManyToManyField(User, related_name="teams")
    name = models.CharField(max_length=255, default="", null=False)
    price = models.IntegerField(null=False, default=0)
    ctime = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = __name__
        db_table = "team"


class Game(models.Model):
    team = models.ForeignKey(Team, related_name="games")
    name = models.CharField(max_length=255, default="", null=False)
    price = models.IntegerField(null=False, default=0)
    ctime = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = __name__
        db_table = "game"


if __name__ == "__main__":
    create_table(User)
    create_table(UserInfo)
    create_table(Team)
    create_table(Game)

    foo = User.objects.create(name="foo")
    UserInfo.objects.create(point=0, user=foo)
    bar = User.objects.create(name="bar")
    UserInfo.objects.create(point=10, user=bar)

    team1 = Team.objects.create(name="team-1")
    Game.objects.create(name="team-1-game-a", team=team1)
    Game.objects.create(name="team-1-game-b", team=team1)
    Game.objects.create(name="team-1-game-c", team=team1)
    team2 = Team.objects.create(name="team-2")
    Game.objects.create(name="team-2-game-a", team=team2)
    Game.objects.create(name="team-2-game-b", team=team2)
    team1.users.add(foo)
    team1.users.add(bar)
    team1.save()
    team2.users.add(bar)
    team2.save()

    import logging
    for name in ['django.db.backends']:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler())

    r = []

    from django_aggressivequery import from_query
    qs = (
        from_query(UserInfo.objects.filter(point__gt=0), ["point", "user__name", "user__teams__name", "user__teams__games__name"], more_specific=True)
        .prefetch_filter(
            user__teams__games=lambda qs: qs.filter(name__contains="-a")
        )
    )
    for info in qs:
        for team in info.user.teams.all():
            for game in team.games.all():
                r.append((info.user.name, info.point, team.name, game.name))
