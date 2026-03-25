from rdkit.Chem import Mol


def merge_compound_to_conformer(compound: Mol, conformer: Mol) -> Mol:
    """
    Merge the compound and conformer records into a single Mol object.
    The compound record contains the 2D structure and metadata, while the conformer record contains the 3D structure.
    This function assumes that the compound and conformer records have been matched by CID and ConfID.

    Args:
        compound (Mol): The compound record (2D structure + metadata).
        conformer (Mol): The conformer record (3D structure).

    Returns:
        Mol: A new Mol object that combines the information from both records.
    """
    # Create a new Mol object to hold the merged data
    merged_mol = Mol(conformer)  # Start with the conformer to retain 3D coordinates

    for prop_name in compound.GetPropNames():
        merged_mol.SetProp(prop_name, compound.GetProp(prop_name))

    return merged_mol
