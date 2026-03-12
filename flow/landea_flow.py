import logging
from typing import Optional

from data_source.landea_data import LandeaScraper


logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("landea_debug.log"),
        logging.StreamHandler(),
    ],
)


class LandeaFlow:
    def __init__(self, max_workers: int = 5, base_url: str | None = None) -> None:
        """
        Simple orchestration flow around LandeaScraper.

        Args:
            max_workers: Number of concurrent workers for enrichment.
            base_url: Optional custom base search URL for Landea.
        """
        self._scraper = LandeaScraper(base_url=base_url, max_workers=max_workers)

    def run_stage1(
        self,
        max_pages: Optional[int] = None,
        start_page: int = 1,
    ) -> int:
        """
        Stage 1:
        - Crawl search result pages from Landea.
        - Upsert listing-level data into the landea_assets table via DAO.

        Args:
            max_pages: Optional cap on how many pages to crawl.
            start_page: First page index to start scraping from.

        Returns:
            Number of rows affected in the DB.
        """
        logger.info(
            "Running Landea Stage 1: start_page=%s, max_pages=%s",
            start_page,
            max_pages,
        )
        affected = self._scraper.save_stage1_to_db(
            max_pages=max_pages,
            start_page=start_page,
        )
        logger.info("Landea Stage 1 complete. Rows affected: %s", affected)
        return affected

    def run_stage2(self, batch_size: int = 100) -> int:
        """
        Stage 2:
        - Load rows missing coordinates from DB.
        - Enrich each asset using its detail page (description, coords, etc.).
        - Upsert enriched rows back into landea_assets.

        Args:
            batch_size: Number of assets to enrich per DB batch.

        Returns:
            Total enriched rows upserted.
        """
        logger.info("Running Landea Stage 2 enrichment with batch_size=%s", batch_size)
        total_updated = self._scraper.enrich_missing_in_db(batch_size=batch_size)
        logger.info("Landea Stage 2 complete. Total enriched rows: %s", total_updated)
        return total_updated


if __name__ == "__main__":
    flow = LandeaFlow(max_workers=5)

    logger.info("\n--- RUNNING LANDEA STAGE 1: SAVE LISTINGS TO DB ---")
    # Example: crawl first 2 pages starting from page 1
    flow.run_stage1(start_page=1, max_pages=2)

    logger.info("\n--- RUNNING LANDEA STAGE 2: ENRICH MISSING COORDS FROM DB ---")
    flow.run_stage2(batch_size=50)

