[![Build Status](https://travis-ci.org/StackVista/sts-agent-integrations-core.svg?branch=master)](https://travis-ci.org/StackVista/sts-agent-integrations-core)

Testing:

# Quick development Setup

To get started developing with the integrations-core repo you will need: `gem`,  `python` and `rvm`.

To install rvm: 
- Make sure you have xcode and accepted the license: `sudo xcode-select --install`
- Run `curl -L https://get.rvm.io | bash -s stable --auto-dotfiles --autolibs=enable --ruby`

We’ve written a gem and a set of scripts to help you get set up, ease development, and provide testing. To begin:

- Run `gem install bundler`
- Run `bundle install`

Once the required Ruby gems have been installed by Bundler, you can easily create a Python environment:

- Run `rake setup_env`. This will install a Python virtual environment along
  with all the components necessary for integration development including the
  core agent used by the integrations. (Note that it clones the agent repo, which branch is used is configured by the env variable `ENV['DD_AGENT_BRANCH']` in the RakeFile.)
  Some basic software might be needed to install the python dependencies like `gcc` and `libssl-dev`.
- Run `source venv/bin/activate` to activate the installed Python virtual
  environment. To exit the virtual environment, run `deactivate`. You can learn
  more about the Python virtual environment on the Virtualenv documentation.

This is a quick setup but from that point you should be able to run the default test suit `rake ci:run`.
To go beyond we advise you to read the full documentation [here](http://docs.datadoghq.com/guides/integration_sdk/).

# Installing the Integration tests

    bundle install
    # When this fails, check whether the python2 executable is available (solvable with symlink)
    # When this fails without venv, just retry with the venv
    bundle exec rake setup_env
    source venv/bin/activate
    ./fix-shadowing.sh

Run tests:

    rake ci:run

Please note that on a macbook not all integration tests will pass, because some tests only work on unix. (e.g. test_cx_state_linux_ss)

For a check with underscores in its name, its package name replaces underscores with dashes. For example, the `powerdns_recursor` check is packaged as `dd-check-powerdns-recursor`.
Run a single test:

    rake ci:run\[haproxy\]

For more details on how the rake infrastructure is set up see the Rakefile and /var/lib/gems/2.3.0/gems/datadog-sdk-testing-0.5.1/lib/sdk.rake

Switching dd-agent branches:

    Update the branch in .travis-ci
    Update the branch in Rakefile
    rm -rf embedded/dd-agent
    Rerun setup_env

# Running the agent from the dev environment

First do the setup of running integration tests

Run `./run.sh install` to install all checks and config files in embedded/dd-agent.
Edit the config file in embedded/dd-agent and embedded/dd-agent/conf.d for the checks you want to run.
Run `./run.sh start` to start and `./run.sh stop` to stop.


# Side notes

if you have situation, when rake commands fail to find `datadog-sdk-testing`,
obvious solution would be use `bundle exec rake <original command>` to ensure,
that gem installed from source is found.

If you experience import errors near `from tests.common`
add agent directory `export PYTHONPATH=...sts-agent-integrations-core/embedded/dd-agent:$PYTHONPATH` 
 
