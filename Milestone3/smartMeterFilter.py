import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class FilterAndConvertDoFn(beam.DoFn):
    def process(self, element):
        try:
            temp = element.get("temperature")
            press = element.get("pressure")
            hum = element.get("humidity")

            if temp is None or press is None or hum is None or temp == "" or press == "" or hum == "":
                logging.info(f"Filtered record (missing data): {element}")
                return

            temp_c = float(temp)
            press_kpa = float(press)
            hum_val = float(hum)

            # P(psi) = P(kPa) / 6.895
            # T(F) = T(C) * 1.8 + 32
            element["pressure"] = press_kpa / 6.895
            element["temperature"] = (temp_c * 1.8) + 32
            element["humidity"] = hum_val

            yield element
        except Exception as e:
            logging.error(f"Error in conversion {element}: {e}")
            return

def run(argv=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--input', dest='input', required=True, help='Input Pub/Sub topic')
    parser.add_argument('--output', dest='output', required=True, help='Output Pub/Sub topic')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input)
            | "To Dict" >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            | "Filter and Convert" >> beam.ParDo(FilterAndConvertDoFn())
            | "To Bytes" >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
            | "Write to Pub/Sub" >> beam.io.WriteToPubSub(topic=known_args.output)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
