import argparse
import importlib.util
import time
from pyspark.sql import SparkSession

def load_stage_function(path):
    spec = importlib.util.spec_from_file_location("stage_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.process  # Each file must define process()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--tasks", type=int, required=True)
    parser.add_argument("--stage_files", nargs='+', required=True)
    parser.add_argument("--args", nargs='+', required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--aggregate_stage", type=int, default=-1,
                        help="0-indexed stage to apply aggregation (optional)")

    args = parser.parse_args()

    start_time = time.time()  # START TIMER

    spark = SparkSession.builder.appName("CustomPipeline").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Load stage functions
    stage_fns = [load_stage_function(f) for f in args.stage_files]
    stage_args = [int(a) if a.isdigit() else a for a in args.args]

    # Load CSV and partition
    rdd = sc.textFile(args.input).map(lambda line: line.split(","))
    rdd = rdd.repartition(args.tasks)

    # Apply stages in order
    for i, stage_fn in enumerate(stage_fns):
        if i == args.aggregate_stage:
            rdd = rdd.map(lambda fields: stage_fn(fields, stage_args[i]))
            rdd = rdd.aggregateByKey(
                0,
                lambda acc, val: acc + val,
                lambda acc1, acc2: acc1 + acc2
            )
            rdd = rdd.map(lambda kv: [kv[0], kv[1]])
        else:
            rdd = rdd.map(lambda fields: stage_fn(fields, stage_args[i]))
            rdd = rdd.filter(lambda x: x is not None)

    # Save output
    rdd.saveAsTextFile(args.output)

    spark.stop()

    end_time = time.time()  # STOP TIMER
    total_time = end_time - start_time
    print(f"Pipeline completed in {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
