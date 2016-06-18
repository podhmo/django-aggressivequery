django-aggressivequery
========================================

(this is experimental package)

handling select_related and prefetch_reated, semi-automatically.


.. code-block:: python

  from django_aggressivequery import from_queryset
  (
      from_queryset(UserInfo.objects.filter(point__gt=0), ["user__teams__games"])
      .prefetch_filter(
          user__teams__games=lambda qs: qs.filter(name__contains="-a")
      )
  )

  # almost same means
  from django.db.models import Prefetch
  (
      UserInfo.objects.filter(point__gt=0)
      .select_related("user")
      .prefetch_related(
          "user__teams",
          Prefetch("user__teams__games", queryset=Game.objects.filter(name__contains="-a"))
      )
  )

SQL example

.. code-block:: sql

  SELECT
   "userinfo"."id", "userinfo"."point", "userinfo"."ctime", "userinfo"."user_id",
   "user"."id", "user"."name", "user"."ctime"
  FROM "userinfo"
  INNER JOIN "user" ON ("userinfo"."user_id" = "user"."id")
  WHERE "userinfo"."point" > 0;
  SELECT
   ("team_users"."user_id") AS "_prefetch_related_val_user_id",
   "team"."id", "team"."name", "team"."price", "team"."ctime"
  FROM "team"
  INNER JOIN "team_users" ON ("team"."id" = "team_users"."team_id")
  WHERE "team_users"."user_id" IN (2);
  SELECT
  "game"."id", "game"."team_id", "game"."name", "game"."price", "game"."ctime"
  FROM "game"
  WHERE ("game"."name" LIKE '%-a%' ESCAPE '\' AND "game"."team_id" IN (1, 2));

model

.. code-block:: python

  # relation: UserInfo - User *-* Team -* Game
  class User(models.Model):
      name = models.CharField(max_length=255, default="", null=False)
      ctime = models.DateTimeField()

  class UserInfo(models.Model):
      point = models.IntegerField(null=False, default=0)
      user = models.OneToOneField(User, related_name="info")
      ctime = models.DateTimeField()

  class Team(models.Model):
      users = models.ManyToManyField(User, related_name="teams")
      name = models.CharField(max_length=255, default="", null=False)
      price = models.IntegerField(null=False, default=0)
      ctime = models.DateTimeField()

  class Game(models.Model):
      team = models.ForeignKey(Team, related_name="games")
      name = models.CharField(max_length=255, default="", null=False)
      price = models.IntegerField(null=False, default=0)
      ctime = models.DateTimeField()

more specific option
----------------------------------------

Calling `from_queryset()` with `more_specific` option, then use `Query.only()`.

.. code-block:: python

  from django_aggressivequery import from_queryset
  (
      from_queryset(UserInfo.objects.filter(point__gt=0), ["point", "user__name", "user__teams__name", "user__teams__games__name"], more_specific=True)
      .prefetch_filter(
          user__teams__games=lambda qs: qs.filter(name__contains="-a")
      )
  )


