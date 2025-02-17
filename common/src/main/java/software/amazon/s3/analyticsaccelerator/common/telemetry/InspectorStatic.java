/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.common.telemetry;

public class InspectorStatic {
  public static StringBuilder inspector;

  public InspectorStatic(StringBuilder inspector) {
    InspectorStatic.inspector = inspector;
  }

  public InspectorStatic() {}

  public static void setInspector(StringBuilder inspector) {
    InspectorStatic.inspector = inspector;
  }

  public static String getInspector() {
    return inspector.toString();
  }

  public static String filterInsepector(String threadName) {
    StringBuilder result = new StringBuilder();
    String[] lines = inspector.toString().split("\n");
    for (String line : lines) {
      if (line.contains(threadName)) {
        result.append(line).append("\n");
      }
    }
    return result.toString();
  }
}
