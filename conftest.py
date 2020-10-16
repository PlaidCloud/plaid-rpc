
import platform

collect_ignore = ["setup.py"]
collect_ignore.append('plaidtools/superset/cli.py')
collect_ignore.append('plaidtools/superset/datasource_helpers.py')
collect_ignore.append('plaidtools/superset/security.py')
collect_ignore.append('plaidtools/superset/superset_config.py')
collect_ignore.append('plaidtools/superset/views.py')
collect_ignore.append('plaidtools/tests/test_template.py')

if platform.system() == "Linux":
    collect_ignore.append("plaidtools/xlwings_utility.py")
    collect_ignore.append("plaidtools/model_analysis.py")

if platform.system() != "Windows":
    collect_ignore.append("plaidtools/pcm_connection.py")
