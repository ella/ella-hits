from hashlib import md5
from datetime import datetime, timedelta

from django.contrib.contenttypes.models import ContentType
from django.db import models
from ella.core.models import Publishable
from ella.core.cache import cache_this
from django.conf import settings
from django.db.models import F, Q
from django.utils.datastructures import SortedDict
from django.utils.translation import ugettext_lazy as _


__author__ = 'xaralis'


def dict_key(kwargs):
    kwargs = SortedDict(kwargs)
    serialize = []

    for key, arg in kwargs.items():
        serialize.append(unicode(key))
        serialize.append(unicode(arg))

    return md5(''.join(serialize)).hexdigest()


def get_top_objects_key(self, days=None, mods=[], excludes={}, **kwargs):
    return 'ella.core.managers.HitCountManager.get_top_objects_key:%d:%s:%s:%s:%s' % (
            settings.SITE_ID,
            str(days),
            ','.join('.'.join(str(model._meta) for model in mods)),
            dict_key(excludes),
            dict_key(kwargs)
        )


class HitCountManager(models.Manager):
    def hit(self, publishable, position=None):
        count = self.filter(publishable=publishable, position=position).update(hits=F('hits') + 1)

        if count < 1:
            self.create(publishable=publishable, hits=1)

    @cache_this(get_top_objects_key)
    def get_top_objects(self, days=None, mods=[], excludes={}, **kwargs):
        """
        Return count top rated objects. Cache this for 10 minutes without any chance of cache invalidation.
        """
        qset = self.filter(publishable__category__site=settings.SITE_ID, **kwargs).order_by('-hits')

        if mods:
            qset = qset.filter(publishable__content_type__in=[ContentType.objects.get_for_model(m) for m in mods])

        now = datetime.now()
        if days is None:
            qset = qset.filter(publishable__publish_from__lte=now)
        else:
            start = now - timedelta(days=days)
            qset = qset.filter(publishable__publish_from__range=(start, now,))
        qset = qset.filter(Q(publishable__publish_to__gt=now) | Q(publishable__publish_to__isnull=True))

        if excludes:
            qset = qset.exclude(**excludes)

        return qset.select_related('publishable')


class HitCount(models.Model):
    """
    Count hits for individual objects.
    """
    publishable = models.ForeignKey(Publishable, primary_key=True)

    position = models.CharField(max_length=255, default=None, blank=True, null=True)

    last_seen = models.DateTimeField(_('Last seen'), editable=False)
    hits = models.PositiveIntegerField(_('Hits'), default=1)

    objects = HitCountManager()

    def save(self, **kwargs):
        "update last seen automaticaly"
        self.last_seen = datetime.now()
        super(HitCount, self).save(**kwargs)

    class Meta:
        verbose_name = _('Hit Count')
        verbose_name_plural = _('Hit Counts')
