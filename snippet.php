<?php

declare(strict_types=1);

namespace App\Service\Statistics;

use App\Entity\Aggregate;
use App\Enum\AggregateReportType;
use App\Searcher\Aggregate\Filter\CampaignStatusFilter;
use App\Searcher\Aggregate\Filter\DateRangeFilter;
use App\Searcher\Aggregate\Filter\LimitFilter;
use App\Searcher\Aggregate\Filter\OffsetFilter;
use App\Searcher\Aggregator\AggregatorBuilder;
use App\Searcher\Filter\FilterBuilder;
use App\Searcher\Sorter\SorterBuilder;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\Query;
use Symfony\Component\HttpFoundation\Request;

/**
 * @author Oghenerie Etoka
 */
readonly class AggregateService
{
    public function __construct(
        private EntityManagerInterface $entityManager,
        private FilterBuilder $filterBuilder,
        private SorterBuilder $sorterBuilder,
        private AggregatorBuilder $aggregatorBuilder,
    ) {
    }

    /**
     * @throws \Exception
     */
    public function getAggregate(Request $request, AggregateReportType $reportType, array $context = []): Query
    {
        $queryBuilder = $this->entityManager->getRepository(Aggregate::class)->createQueryBuilder('a');

        $context = array_merge($context, $this->getContext($reportType));

        $context['format'] = $request->getPreferredFormat();

        // Add filters
        foreach ($this->filterBuilder->fetch(Aggregate::class, $context) as $filter) {
            $filter->apply($queryBuilder, $request, $context);
        }

        // Add sorters
        foreach ($this->sorterBuilder->fetch(Aggregate::class, $context) as $sorter) {
            $sorter->apply($queryBuilder, $request, $context);
        }

        // Add aggregator
        $this->aggregatorBuilder
            ->getAggregator(Aggregate::class, $reportType->value)
            ?->aggregate($queryBuilder, $context);

        return $queryBuilder->getQuery();
    }

    /**
     * @throws \Exception
     */
    private function getContext(AggregateReportType $reportType): array
    {
        return match ($reportType) {
            AggregateReportType::DAILY, AggregateReportType::OLD_DAILY => [
                'cursorColumn' => 'date',
                'aggregateBy' => $reportType->value,
                'customLimit' => true,
                'skipFilter' => [
                    CampaignStatusFilter::class,
                    DateRangeFilter::class,
                ],
            ],
            AggregateReportType::TRACKING_TYPE => [
                'aggregateBy' => $reportType->value,
                'customLimit' => true,
                'skipFilter' => [
                    CampaignStatusFilter::class,
                    LimitFilter::class,
                ],
            ],
            AggregateReportType::ADVERTISER_PERFORMANCE, AggregateReportType::PUBLISHER_PERFORMANCE => [
                'skipFilter' => [
                    LimitFilter::class,
                ],
                'customLimit' => true,
            ],
            AggregateReportType::PERFORMANCE_COUNT => [
                'skipFilter' => [
                    LimitFilter::class,
                    OffsetFilter::class,
                ],
                'customLimit' => true,
                'skipSorter' => 'all',
            ],
        };
    }
