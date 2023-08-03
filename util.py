from classes import *


def object_to_cypher_structure(object: object) -> str:

    if type(object) not in [Genre, AlbumType, ReleaseDatePrecision, Artist, Content, AudioFeature]:
        return str(object) if type(object) in [int, float]\
              else '\"'+str(object)+'\"' if type(object) is not list \
                else '['+str(','.join(list(map(object_to_cypher_structure, object))))+']'

    cypher_string = ''
    object_items = list(enumerate(object.__dict__.items()))

    for index, (key,value) in object_items:
        value = object_to_cypher_structure(value)
        cypher_string += f"{key}: {value}{', ' if index < len(object_items)-1 else ''}"

    return '{'+cypher_string+'}'

teste = Content("aaaa","pedro","album",15,["BR"],"15-01-2020","date","url","group")
#print(teste)

print(object_to_cypher_structure(teste))