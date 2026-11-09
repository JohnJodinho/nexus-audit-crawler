from app.api.routes.exports import _export_file_id


def test_export_file_id_fits_appwrite_limit_and_preserves_job_uniqueness():
    job_id = "export-7ff3cb30449b4b19bba01ecf3261cd34"

    file_id = _export_file_id(job_id)

    assert file_id == "exp_7ff3cb30449b4b19bba01ecf3261cd34"
    assert len(file_id) == 36
