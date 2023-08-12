/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.custom.custom.file.rfile.uids;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Abstract implementation of the UIDBuilder
 *
 * @param <UID_TYPE> - type of the AbstractUIDBuilder
 */
@SuppressWarnings({"unchecked", "deprecation", "static-method", "static"})
public abstract class AbstractUIDBuilder<UID_TYPE extends UID> implements UIDBuilder<UID_TYPE> {

  private static final Logger LOGGER = Logger.getLogger(AbstractUIDBuilder.class);

  @Override
  public void configure(final Configuration config, final Option... options) {
    if (null != config) {
      // Get the UID-specific options
      final Map<String,Option> uidOptions;
      if (null != options) {
        uidOptions = new HashMap<>(4);
        for (final Option option : options) {
          if (null != option) {
            // Look for one of the 4 types of UID options
            final String key = option.getLongOpt();
            final String value;
            if (UIDConstants.UID_TYPE_OPT.equals(key)) {
              value = option.getValue(HashUID.class.getSimpleName());
            } else if (UIDConstants.HOST_INDEX_OPT.equals(key)) {
              value = option.getValue();
            } else if (UIDConstants.PROCESS_INDEX_OPT.equals(key)) {
              value = option.getValue();
            } else if (UIDConstants.THREAD_INDEX_OPT.equals(key)) {
              value = option.getValue();
            } else {
              value = null;
            }

            // Check for null
            if (null != value) {
              // Put the key and value into the map
              uidOptions.put(key, option);

              // Stop looping if we've got everything we need
              if (uidOptions.size() >= 4) {
                break;
              } else if (UIDConstants.UID_TYPE_OPT.equals(key)
                  && HashUID.class.getSimpleName().equals(value)) {
                break;
              }
            }
          }
        }
      } else {
        uidOptions = Collections.emptyMap();
      }

      // Configure with the UID-specific options
      configure(config, uidOptions);
    }
  }

  /*
   * Validate and configure UID properties
   *
   * @param config Hadoop configuration
   *
   * @param options the UID-specific configuration options
   */
  private void configure(final Configuration config, final Map<String,Option> options) {
    // Assign and validate the option value for the UID type
    final Option option = options.get(UIDConstants.UID_TYPE_OPT);
    String uidType = (null != option) ? option.getValue() : null;
    if (null == uidType) {
      uidType = HashUID.class.getSimpleName();
      if (LOGGER.isDebugEnabled()) {
        final String message = "Defaulting configuration to UID type "
            + HashUID.class.getSimpleName() + " due to unspecified value";
        LOGGER.info(message);
      }
    } else if (SnowflakeUID.class.getSimpleName().equals(uidType)) {
      if (options.size() < 4) {
        uidType = HashUID.class.getSimpleName();
        final String message = "Unable to configure UID type " + SnowflakeUID.class.getSimpleName();
        LOGGER.warn(message,
            new IllegalArgumentException("Insufficient number of 'Snowflake' options: " + options));
      }
    } else if (!HashUID.class.getSimpleName().equals(uidType)) {
      final String invalidType = uidType;
      uidType = HashUID.class.getSimpleName();
      final String message = "Defaulting configuration to UID type " + HashUID.class.getSimpleName()
          + " due to unspecified value";
      LOGGER.warn(message, new IllegalArgumentException("Unrecognized UID type: " + invalidType));
    }
    config.set(UIDConstants.CONFIG_UID_TYPE_KEY, uidType, this.getClass().getName());

    // Configure Snowflake machine ID
    if (SnowflakeUID.class.getSimpleName().equals(uidType)) {
      int machineId = SnowflakeUIDBuilder.newMachineId(options);
      if (machineId >= 0) {
        if (LOGGER.isDebugEnabled()) {
          final String message = "Setting configuration " + config.hashCode() + " to use "
              + SnowflakeUIDBuilder.class.getSimpleName() + " based on UID type " + uidType
              + " and machine ID " + machineId;
          LOGGER.debug(message);
        }
        config.setInt(UIDConstants.CONFIG_MACHINE_ID_KEY, machineId);
      } else if (LOGGER.isDebugEnabled()) {
        final String message =
            "Unable to set configuration to use " + SnowflakeUIDBuilder.class.getSimpleName()
                + " based on UID type " + uidType + " with machine ID " + machineId;
        LOGGER.warn(message);
        config.set(UIDConstants.CONFIG_UID_TYPE_KEY, HashUID.class.getSimpleName(),
            this.getClass().getName());
      }
    }
  }

  @SuppressWarnings({"unchecked", "static-access"})
  @Override
  public UID_TYPE newId(final UID template, final String... extras) {
    // Validate to account for edge cases like StringUID
    final UID validatedTemplate;
    if (null != template) {
      if (template.getClass() == HashUID.class) {
        validatedTemplate = template;
      } else if (template.getClass() == SnowflakeUID.class) {
        validatedTemplate = template;
      } else {
        validatedTemplate = template.parse(template.toString());
      }
    } else {
      validatedTemplate = null;
    }

    // Return the validated template
    final UID_TYPE returnable;
    if (validatedTemplate instanceof HashUID) {
      returnable = (UID_TYPE) HashUIDBuilder.newId((HashUID) template, extras);
    } else if (validatedTemplate instanceof SnowflakeUID) {
      returnable = (UID_TYPE) SnowflakeUIDBuilder.newId((SnowflakeUID) template, extras);
    } else {
      returnable = (UID_TYPE) validatedTemplate;
    }

    return returnable;
  }
}
