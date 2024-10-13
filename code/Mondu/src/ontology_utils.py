import owlready2 as owl
import tempfile
from typing import Dict, Any

def setup_ontology(config: Dict[str, Any]) -> owl.Ontology:
    """
    Set up the ontology based on the configuration.

    :param config: Configuration dictionary
    :return: Loaded ontology object
    """
    owl.onto_path.append(config['ONTO_PATH'])
    world = owl.default_world
    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as temp_file:
        world.set_backend(filename=temp_file.name)
        onto = world.get_ontology(config['COPH_IRI']).load()
    return onto

def search_coph_ontology(onto_fields: List[str], ontology: owl.Ontology) -> Dict[str, Any]:
    """
    Search COPH ontology for suitable terms.

    :param onto_fields: List of fields to search for
    :param ontology: Owlready2 ontology object
    :return: Dictionary of new mappings
    """
    new_mappings = {}
    for field in onto_fields:
        query = input(f"Type an alternative term to search for {field}, or leave blank to use field name: ") or field
        onto_result = ontology.search(label=f"*{query}*") or ontology.search(comment=f"*{query}*")
        
        if onto_result:
            print(f"Resulting option(s) for '{query}' is/are:\n")
            for num, result in enumerate(onto_result):
                print(f"{num}) {result.label}: {result.iri}")
            chosen_match = input("Please type the number of the chosen response (empty implies none): ")
            if chosen_match:
                mapping_choice = onto_result[int(chosen_match)]
                new_mappings[mapping_choice.label[0]] = mapping_choice.iri
        else:
            print("No suitable option was found")
            new_mappings.update(prompt_manual_mapping(field))
    
    return new_mappings

def search_ols(onto_fields: List[str], ontology: owl.Ontology) -> Dict[str, Any]:
    """
    Search OLS (Ontology Lookup Service) for suitable terms.

    :param onto_fields: List of fields to search for
    :param ontology: Owlready2 ontology object
    :return: Dictionary of new mappings
    """
    new_mappings = {}
    for field in onto_fields:
        new_mapping = ols_search(field=field, ontology=ontology)
        new_mappings[new_mapping[field]] = new_mapping[field]
    return new_mappings

def ols_search(field: str, ontology: owl.Ontology) -> Dict[str, str]:
    """
    Perform an OLS search for a given field.

    :param field: Field to search for
    :param ontology: Owlready2 ontology object
    :return: Dictionary containing the mapping for the field
    :raises requests.RequestException: If the OLS search request fails
    """
    print("\nField: "+field)
    query = input("Type an alternative term to search for, or leave blank to use field name: ") or field
    
    url_queries = []
    if input("\nWould you like to filter to exact matches? [Y/n]").lower() not in ('n', 'no'):
        url_queries.append("exact=true")
    
    query_field_query = input("\nWhat would you like to query on?: \n  1) Label\n  2) Synonym\n  3) Both\nOption: ")
    if query_field_query == "1":
        url_queries.append("queryFields=label")
    elif query_field_query == "2":
        url_queries.append("queryFields=synonym")
    else:
        url_queries.append("queryFields=label,synonym")
    
    api_url = f"https://www.ebi.ac.uk/ols/api/search?q={query}&{'&'.join(url_queries)}&fieldList=iri,label,ontology_name,description"
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        options = response.json()

        print("\nHere are the first 10 results:")
        for num, result in enumerate(options['response']['docs'][:10]):
            print(f"{num}) IRI: {result['iri']}\n   label: {result['label']}\n   ontology: {result['ontology_name']}\n   Description: {result['description']}\n")
        
        selected_mapping = input("Please choose the number of the result you wish to use (leave blank for none): ")
        if selected_mapping and int(selected_mapping) < 10:
            selected_result = options['response']['docs'][int(selected_mapping)]
            if ontology.search(label=f"*{selected_result['label']}*") is None:
                with ontology:
                    owl.types.new_class(selected_result['label'], (ontology['Thing'],))
            return {field: selected_result['iri']}
        else:
            print("None chosen, you will now be asked to provide your own mapping")
            return prompt_manual_mapping(query)
    except requests.RequestException as e:
        print(f"OLS search failed: {str(e)}")
        print("No response from ontology search service")
        return prompt_manual_mapping(query)

def prompt_manual_mapping(field: str) -> Dict[str, str]:
    """
    Prompt user for manual mapping of a field.

    :param field: Field to map
    :return: Dictionary of new mapping
    """
    while True:
        manual_label = input(f"Please type the label to use for {field}: ")
        manual_iri = input(f"Please type the iri to match with {manual_label} for {field}: ")
        prompt_verification = input(f"{manual_label}: {manual_iri}; are these correct? (Y/n)")
        if prompt_verification.lower() not in ('n', 'no'):
            return {manual_label: manual_iri}