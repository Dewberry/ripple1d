from ripple1d.ops.ras_conflate import conflate_model
from tests.conflation_tests.classes import ConflationFile, PathManager
from tests.conflation_tests.plotting import plot_conflation


# @pytest.mark.parametrize(["test_a"])
def run_scenario(ras_dir_name: str):
    """Run a specific test case."""
    pm = PathManager(ras_dir_name)
    conflate_model(pm.ras_dir, pm.ras_model_name, pm.nwm_dict)
    plot_conflation(pm.ras_path, pm.nwm_path, pm.conflation_file)
    ConflationFile(pm.conflation_file) == ConflationFile(pm.rubric_file)  # Validation


def run_all():
    """Run all conflation tests."""
    # for test in ["test_a", "test_b", "test_c", "test_d"]:
    for test in ["test_f"]:
        run_scenario(test)


if __name__ == "__main__":
    run_all()
