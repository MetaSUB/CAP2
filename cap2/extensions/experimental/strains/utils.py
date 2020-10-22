

def clean_microbe_name(microbe_name):
    microbe_name = microbe_name.replace(' ', '_')
    microbe_name = microbe_name.replace('-', '_')
    microbe_name = microbe_name[0].upper() + microbe_name[1:].lower()
    return microbe_name
