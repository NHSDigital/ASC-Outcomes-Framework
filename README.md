# Adult Social Care Outcome Framework
The Adult Social Care Outcomes Framework (ASCOF) measures how well care and support services achieve the outcomes that matter most to people. The ASCOF is used both locally and nationally to set priorities for care and support, measure progress and strengthen transparency and accountability.


# Initial package set up

Run the following command to set up your environment correctly **from the root directory**

```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If, while developing this package, you change the installed packages, please update the environment file using

```
pip list --format=freeze > requirements.txt
```

## VSCode specific setup

For Visual Studio Code it is necessary that you change your default interpreter to be the virtual environment you just created `.venv`. To do this use the shortcut `Ctrl-Shift-P`, search for `Python: Select interpreter` and select `.venv` from the list.

# Git Hook Setup

Please running the following command to setup the Git Hooks.

```
python .\scripts\setup-hooks.py
```

You will now be prompted when committing to add a JIRA ticket number to your commit message.

**Please do not use the VS Code Git Tab to commit as this will no longer work.**

_However, you can use it for adding files to be committed._

# Running the code

Please check that the settings, including the path to the input files, are correct in `ascof/params.py`.

You can then create the publication (from the base directory) using

```
python -m ascof.create_ascof
```

# Important things to know


# Link to the publication

Report:
https://digital.nhs.uk/data-and-information/publications/statistical/adult-social-care-outcomes-framework-ascof

# Authors
Liz Selfridge, Luke Atkinson, Tomide Adeyeye

Repo Owner Contact Details: socialcare.statistics@nhs.net

# Licence

The Personal Social Services Adult Social Care Overview publication codebase is released under the MIT License.
The documentation is © Crown copyright and available under the terms of the Open Government 3.0 licence.
