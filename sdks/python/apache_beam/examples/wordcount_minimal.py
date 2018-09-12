#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A minimalist word-counting workflow that counts words in Shakespeare.

This is the first in a series of successively more detailed 'word count'
examples.

Next, see the wordcount pipeline, then the wordcount_debugging pipeline, for
more detailed examples that introduce additional concepts.

Concepts:

1. Reading data from text files
2. Specifying 'inline' transforms
3. Counting a PCollection
4. Writing data to Cloud Storage as text files

To execute this pipeline locally, first edit the code to specify the output
location. Output location could be a local file path or an output prefix
on GCS. (Only update the output location marked with the first CHANGE comment.)

To execute this pipeline remotely, first edit the code to set your project ID,
runner type, the staging location, the temp location, and the output location.
The specified GCS bucket(s) must already exist. (Update all the places marked
with a CHANGE comment.)

Then, run the pipeline as described in the README. It will be deployed and run
using the Google Cloud Dataflow Service. No args are required to run the
pipeline. You can see the results in your output bucket in the GCS browser.
"""

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      # CHANGE 1/5: The Google Cloud Storage path is required
                      # for outputting the results.
                      default='./result_wordcount_minimal',
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  '''
  parse_known_args: does not produce an error when extra arguments are present.
  known_args:       --input & --output
  pipeline_args:    unknown args.
  '''

  pipeline_args.extend([ # extend is similar to append. It only append elements to args (better).
      # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DirectRunner',
      # CHANGE 3/5: Your project ID is required in order to run your pipeline on
      # the Google Cloud Dataflow Service.
      '--project=SET_YOUR_PROJECT_ID_HERE',
      # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
      # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
      '--job_name=your-wordcount-job',
  ])

  '''
  pipeline_options: setting runner based configurations.
  This object lets us set various options for our pipeline, such as the pipeline runner that will execute our pipeline,
  and any runner-specific configuration required by the chosen runner.
  '''
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)  # pipeline_args check above
  pipeline_options.view_as(SetupOptions).save_main_session = True

  '''
  Create a Pipeline object with the options we’ve just constructed.
  The Pipeline object builds up the graph of transformations to be executed, associated with that particular pipeline.
  '''
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.

    lines = p | ReadFromText(known_args.input)
    # p | beam.io.ReadFromText('gs://dataflow-samples/shakespeare/kinglear.txt')

    # Count the occurrences of each word.
    counts = (
        lines
        # This transform splits the lines in PCollection<String>, where each element is an individual word in Shakespeare’s collected texts.
        | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))  # re.findall: find all words and list them seperativly
                      .with_output_types(unicode))
        # The map transform is a higher-level composite transform that encapsulates a simple ParDo.
        # For each element in the input PCollection, the map transform applies a function that produces exactly one output element.
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))

    # Format the counts into a PCollection of strings.
    def format_result(word_count):
      (word, count) = word_count
      return '%s: %s' % (word, count)

    output = counts | 'Format' >> beam.Map(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | WriteToText(known_args.output)
    # A text file write transform. This transform takes the final PCollection of formatted Strings as input and writes each element to an output text file.
    # Each element in the input PCollection represents one line of text in the resulting output file.

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
