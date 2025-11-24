<?php
/*
 * (c) webfactory GmbH <info@webfactory.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Webfactory\ContentMapping\DestinationAdapter\Solarium;

use Iterator;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Solarium\Core\Client\ClientInterface;
use Solarium\QueryType\Select\Result\Result as SelectResult;
use Solarium\Core\Query\DocumentInterface;
use Solarium\QueryType\Update\Query\Document as ReadWriteDocument;
use Webfactory\ContentMapping\DestinationAdapter;
use Solarium\QueryType\Update\Query\Document\Document;
use Webfactory\ContentMapping\ProgressListenerInterface;
use Webfactory\ContentMapping\UpdateableObjectProviderInterface;

/**
 * Adapter for the solarium Solr client as a destination system.
 *
 * @template-implements DestinationAdapter<DocumentInterface, ReadWriteDocument>
 * @template-implements UpdateableObjectProviderInterface<DocumentInterface, ReadWriteDocument>
 */
final class SolariumDestinationAdapter implements DestinationAdapter, ProgressListenerInterface, UpdateableObjectProviderInterface
{
    /**
     * @var ClientInterface
     */
    private $solrClient;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var int Number of documents to collect before flushing intermediate results to Solr.
     */
    private $batchSize;

    /**
     * @var DocumentInterface[]
     */
    private $newOrUpdatedDocuments = [];

    /**
     * @var string[]|int[]
     */
    private $deletedDocumentIds = [];

    public function __construct(ClientInterface $solrClient, LoggerInterface $logger = new NullLogger(), int $batchSize = 20)
    {
        $this->solrClient = $solrClient;
        $this->logger = $logger;
        $this->batchSize = $batchSize;
    }

    public function getObjectsOrderedById(string $className): Iterator
    {
        $normalizedObjectClass = $this->normalizeObjectClass($className);
        $query = $this->solrClient->createSelect()
            ->setQuery('objectclass:'.$normalizedObjectClass)
            ->setStart(0)
            ->setRows(1000000)
            ->setFields(['id', 'objectid', 'objectclass', 'hash'])
            ->addSort('objectid', 'asc');

        /** @var SelectResult $resultset */
        $resultset = $this->solrClient->execute($query);

        $this->logger->info(
            "SolariumDestinationAdapter found {number} objects for objectClass {objectClass}",
            [
                'number' => $resultset->getNumFound(),
                'objectClass' => $className,
            ]
        );

        /** @var Iterator<DocumentInterface> */
        return $resultset->getIterator();
    }

    public function createObject(int $id, string $className): ReadWriteDocument
    {
        $normalizedObjectClass = $this->normalizeObjectClass($className);

        $updateQuery = $this->solrClient->createUpdate();

        /** @var ReadWriteDocument */
        $newDocument = $updateQuery->createDocument();
        $newDocument->id = $normalizedObjectClass.':'.$id;
        $newDocument->objectid = $id;
        $newDocument->objectclass = $normalizedObjectClass;

        return $newDocument;
    }

    public function prepareUpdate(object $destinationObject): ReadWriteDocument
    {
        return new ReadWriteDocument($destinationObject->getFields());
    }

    public function delete(object $objectInDestinationSystem): void
    {
        $this->deletedDocumentIds[] = $objectInDestinationSystem->id;
    }

    /**
     * This method is a hook e.g. to notice an external change tracker that the $object has been updated.
     */
    public function updated(object $objectInDestinationSystem): void
    {
        if (!$objectInDestinationSystem instanceof ReadWriteDocument) {
            throw new \InvalidArgumentException();
        }

        $this->newOrUpdatedDocuments[] = $objectInDestinationSystem;
    }

    public function afterObjectProcessed(): void
    {
        if ((count($this->deletedDocumentIds) + count($this->newOrUpdatedDocuments)) >= $this->batchSize) {
            $this->flush();
        }
    }

    public function commit(): void
    {
        $this->flush();
    }

    /**
     * @inheritdoc
     */
    public function idOf(object $objectInDestinationSystem): int
    {
        if (!isset($objectInDestinationSystem->objectid)) {
            throw new InvalidArgumentException();
        }

        return $objectInDestinationSystem->objectid;
    }

    private function flush(): void
    {
        $this->logger->info(
            "Flushing {numberInsertsUpdates} inserts or updates and {numberDeletes} deletes",
            [
                'numberInsertsUpdates' => count($this->newOrUpdatedDocuments),
                'numberDeletes'        => count($this->deletedDocumentIds),
            ]
        );

        if (count($this->deletedDocumentIds) === 0 && count($this->newOrUpdatedDocuments) === 0) {
            return;
        }

        $updateQuery = $this->solrClient->createUpdate();

        if ($this->deletedDocumentIds) {
            $updateQuery->addDeleteByIds($this->deletedDocumentIds);
        }

        if ($this->newOrUpdatedDocuments) {
            $updateQuery->addDocuments($this->newOrUpdatedDocuments);
        }

        $updateQuery->addCommit();
        $this->solrClient->execute($updateQuery);

        $this->deletedDocumentIds = [];
        $this->newOrUpdatedDocuments = [];

        $this->logger->debug("Flushed");

        /*
         * Manually trigger garbage collection
         * \Solarium\Core\Query\AbstractQuery might hold a reference
         * to a "helper" object \Solarium\Core\Query\Helper, which in turn references
         * back the document. This circle prevents the normal, refcount-based GC from
         * cleaning up the processed Document instances after we release them.
         *
         * To prevent memory exhaustion, we start a GC cycle collection run.
         */
        $updateQuery = null;
        gc_collect_cycles();
    }

    private function normalizeObjectClass(string $objectClass): string
    {
        if (substr($objectClass, 0, 1) === '\\') {
            $objectClass = substr($objectClass, 1);
        }

        return str_replace('\\', '-', $objectClass);
    }
}
