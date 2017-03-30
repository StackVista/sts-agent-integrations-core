[![Build Status](https://travis-ci.org/StackVista/sts-agent-integrations-core.svg?branch=master)](https://travis-ci.org/StackVista/sts-agent-integrations-core)

Testing:

Setup:

    bundle install
    # When this fails, check whether the python2 executable is available (solvable with symlink)
    rake setup_env
    source venv/bin/activate

Run tests:

    rake ci:run

Run a single test:

    rake ci:run\[haproxy\]

For more details on how the rake infrastructure is set up see the Rakefile and /var/lib/gems/2.3.0/gems/datadog-sdk-testing-0.5.1/lib/sdk.rake
