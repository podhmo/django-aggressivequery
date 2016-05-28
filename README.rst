django-aggressivequery
========================================

handling select_related and prefetch_reated, semi-automatically.


.. code-block:: python

  from django_aggressivequery import from_query
  (
      from_query(UserInfo.objects.filter(point__gt=0), ["user__teams__games"])
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
          "user__teams__games",
          Prefetch("user__teams__games", queryset=Game.objects.filter(name__contains="-a"))
      )
  )

SQL example ::

  SELECT "userinfo"."id", "userinfo"."point", "userinfo"."user_id", "user"."id", "user"."name", "user"."memo3" FROM "userinfo" INNER JOIN "user" ON ("userinfo"."user_id" = "user"."id") WHERE "userinfo"."point" > 0
  SELECT ("team_users"."user_id") AS "_prefetch_related_val_user_id", "team"."id", "team"."name", "team"."price", "team"."memo3" FROM "team" INNER JOIN "team_users" ON ("team"."id" = "team_users"."team_id") WHERE "team_users"."user_id" IN (2)
  SELECT "game"."id", "game"."team_id", "game"."name", "game"."price" FROM "game" WHERE ("game"."name" LIKE '%-a%' ESCAPE '\' AND "game"."team_id" IN (1, 2))

model

.. code-block:: python

  # relation: UserInfo - User *-* Team -* Game
  class User(models.Model):
      name = models.CharField(max_length=255, default="", null=False)

  class UserInfo(models.Model):
      point = models.IntegerField(null=False, default=0)
      user = models.OneToOneField(User, related_name="info")

  class Team(models.Model):
      users = models.ManyToManyField(User, related_name="teams")
      name = models.CharField(max_length=255, default="", null=False)
      price = models.IntegerField(null=False, default=0)

  class Game(models.Model):
      team = models.ForeignKey(Team, related_name="games")
      name = models.CharField(max_length=255, default="", null=False)
      price = models.IntegerField(null=False, default=0)
