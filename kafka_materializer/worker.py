from typing import Dict, List
from kafka import KafkaConsumer
from kafka_materializer.data.materializer import MaterializerDataService
from kafka_materializer.settings import Settings
import logging
import orjson

from kafka_materializer.types import View

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, settings: Settings):
        self._settings = settings

    def __call__(self):
        mds = MaterializerDataService(self._settings)

        cursor, views = mds.get_views()
        while cursor is not None:
            cursor, more_views = mds.get_views(cursor=cursor)
            views.extend(more_views)

        consumer = KafkaConsumer(
            *[v.topic for v in views],
            bootstrap_servers=self._settings.kafka_servers,
            group_id="materializer",
            auto_offset_reset="earliest",
        )

        mapped_views: Dict[str, List[View]] = {}
        for view in views:
            if view.topic not in mapped_views:
                mapped_views[view.topic] = []
            mapped_views[view.topic].append(view)

        count = 0
        for msg in consumer:
            views = mapped_views[msg.topic]
            for view in views:
                try:
                    data = orjson.loads(msg.value)
                    fields = {}
                    for key in view.fields:
                        if key in data:
                            fields[key] = data[key]
                    mds.create_material(
                        view.id,
                        data[view.order_by],
                        orjson.dumps(fields).decode("utf-8"),
                    )
                except BaseException:
                    logger.error(
                        f"Error processing material: {view}, {msg}", exc_info=True
                    )

            count += 1
            if count % 100 == 0:
                print(f"Processed {count}")
