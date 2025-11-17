# content-mapping-destinationadapter-solarium #

`DestinationAdapter` for the [webfactory/content-mapping](https://github.com/webfactory/content-mapping)
mini framework. Uses the [Solarium Solr client](https://github.com/solariumphp/solarium) to map content to a Solr backend.

## Usage 

```php
use Solarium\Client;
use Webfactory\ContentMapping\Synchronizer;
use Webfactory\ContentMapping\Solr\SolariumDestinationAdapter;

$solrClient = new Client($configArray); // see Solarium documentation for details

$synchronizer = new Synchronizer($sourceAdapter, $mapper, new SolariumDestinationAdapter($solrClient));
```

## Credits, Copyright and License 

This project was started at webfactory GmbH, Bonn.

- <https://www.webfactory.de>

Copyright 2015-2025 webfactory GmbH, Bonn. Code released under [the MIT license](LICENSE).
