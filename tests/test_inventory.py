from uuid import uuid4

from app.inventory import candidate_inventory_query, inventory_values


def test_inventory_values_preserves_original_and_normalizes_url():
    values = inventory_values(
        crawl_uuid=uuid4(),
        original_url="HTTPS://Example.COM/courses/?tag=python#intro",
        discovery_status="queued",
        processing_status="queued",
    )

    assert values["original_url"].endswith("#intro")
    assert values["normalized_url"] == "https://example.com/courses?tag=python"
    assert values["origin"] == "https://example.com"
    assert values["normalized_path"] == "/courses"
    assert values["query_parameters"] == {"tag": "python"}


def test_inventory_candidate_query_scopes_crawl_origin_and_path():
    statement = candidate_inventory_query(
        crawl_uuid=uuid4(),
        origin="https://example.com",
        path_prefix="/courses",
    )

    sql = str(statement)
    assert "url_inventory.crawl_id" in sql
    assert "url_inventory.origin" in sql
    assert "url_inventory.normalized_path" in sql
