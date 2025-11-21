# tests/test_conftest.py

def test_schema_classes_fixture(schema_classes):
    """Verify schema classes are discovered."""
    assert len(schema_classes) > 0
    print(f"\nDiscovered {len(schema_classes)} schema classes:")
    for cls in schema_classes:
        print(f"  - {cls.__name__}")


def test_schema_mocks_fixture(schema_mocks):
    """Verify schema mocks are available."""
    assert len(schema_mocks) > 0
    print(f"\nFound {len(schema_mocks)} mock schemas:")
    for schema_cls in schema_mocks.keys():
        print(f"  - {schema_cls.__name__}")


def test_fixtures_alignment(schema_classes, schema_mocks):
    """Check if all schema classes have mocks."""
    classes_set = set(schema_classes)
    mocks_set = set(schema_mocks.keys())
    
    missing_mocks = classes_set - mocks_set
    extra_mocks = mocks_set - classes_set
    
    if missing_mocks:
        print(f"\n⚠️  Schemas without mocks: {[c.__name__ for c in missing_mocks]}")
    
    if extra_mocks:
        print(f"\n⚠️  Mocks without schemas: {[c.__name__ for c in extra_mocks]}")
    
    print(f"\n✓ Coverage: {len(mocks_set)}/{len(classes_set)} schemas have mocks")