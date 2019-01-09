# target-s3

This is a [Singer](https://singer.io) target that reads JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).


---

Copyright &copy; 2018 Stitch



running 
Facebook tap example:

tap-facebook -c /home/vitor/projects/indicium-etl/extractions/taps/tap-facebookads/facebook-conf.json -p /home/vitor/projects/indicium-etl/extractions/taps/tap-facebookads/catalog-age-and-gender.json --catalog /home/vitor/projects/indicium-etl/extractions/taps/tap-facebookads/catalog-age-and-gender.json | python /home/vitor/projects/target-s3/target_s3/__init__.py -c /home/vitor/projects/target-s3/facebook-conf.json 




questions:

what happens when 