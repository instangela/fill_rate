# ml-transforms

Machine learning transforms for Instawork. The T in EL(T)

### Install `dbt-redshift` using Homebrew

```bash
brew update
brew tap dbt-labs/dbt
brew install dbt-redshift

dbt --version # should print properly
dbt deps      # will download dbt_utils, dbt_redshift macros, etc
```

### Setup your environment

You can run `./scripts/setup.sh` to setup your dbt profile and `mole`

```bash
# ./scripts/setup.sh [dbt-username] [redshift-username] [redshift-password]

eric@erics-m1x ~/p/ml-transforms (main) [1]> ./scripts/setup.sh ehagman ml aBC1de2Fg3
~/.dbt/profiles.yml generated
```

### Use `mole` to connect to the data warehouse

You need to connect via Bastion to our Redshift DW. `mole` is used to automatically connect. Run:

```
mole start alias instawork-dw
```

This will run the SSH tunnel in the foreground. You can now run `dbt run`!

### Using the project

Try running the following commands:

- dbt run
- dbt test

### Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
