# movies/templatetags/mongo_extras.py
from django import template

register = template.Library()

@register.filter(name='get_id')
def get_id(value):
    """Filtre pour récupérer l'attribut _id d'un document MongoDB"""
    return value.get('_id')