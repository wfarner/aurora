/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.common.application.modules;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.application.ShutdownStage;
import org.apache.aurora.common.application.StartupRegistry;
import org.apache.aurora.common.application.StartupStage;

import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.base.ExceptionalCommand;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Binding module for startup and shutdown controller and registries.
 *
 * Bindings provided by this module:
 * <ul>
 *   <li>{@code @StartupStage ExceptionalCommand} - Command to execute all startup actions.
 *   <li>{@code ShutdownRegistry} - Registry for adding shutdown actions.
 *   <li>{@code @ShutdownStage Command} - Command to execute all shutdown commands.
 * </ul>
 *
 * If you would like to register a startup action that starts a local network service, please
 * consider using {@link LocalServiceRegistry}.
 *
 * @author William Farner
 */
public class LifecycleModule extends AbstractModule {

  /**
   * Binding annotation used for local services.
   * This is used to ensure the LocalService bindings are visibile within the package only, to
   * prevent injection inadvertently triggering a service launch.
   */
  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface Service { }

  @Override
  protected void configure() {
    bind(Lifecycle.class).in(Singleton.class);

    bind(Key.get(ExceptionalCommand.class, StartupStage.class)).to(StartupRegistry.class);
    bind(StartupRegistry.class).in(Singleton.class);

    bind(ShutdownRegistry.class).to(ShutdownRegistry.ShutdownRegistryImpl.class);
    bind(Key.get(Command.class, ShutdownStage.class)).to(ShutdownRegistry.ShutdownRegistryImpl.class);
    bind(ShutdownRegistry.ShutdownRegistryImpl.class).in(Singleton.class);
    bindStartupAction(binder(), ShutdownHookRegistration.class);

    bind(LocalServiceRegistry.class).in(Singleton.class);

    // Ensure that there is at least an empty set for the service runners.
    runnerBinder(binder());

    bindStartupAction(binder(), LocalServiceLauncher.class);
  }

  /**
   * Thrown when a local service fails to launch.
   */
  public static class LaunchException extends Exception {
    public LaunchException(String msg) {
      super(msg);
    }

    public LaunchException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /**
   * Responsible for starting and stopping a local service.
   */
  public interface ServiceRunner {

    /**
     * Launches the local service.
     *
     * @return Information about the launched service.
     * @throws LaunchException If the service failed to launch.
     */
    LocalServiceRegistry.LocalService launch() throws LaunchException;
  }

  @VisibleForTesting
  static Multibinder<ServiceRunner> runnerBinder(Binder binder) {
    return Multibinder.newSetBinder(binder, ServiceRunner.class, Service.class);
  }

  /**
   * Adds a startup action to the startup registry binding.
   *
   * @param binder Binder to bind against.
   * @param actionClass Class to bind (and instantiate via guice) for execution at startup.
   */
  public static void bindStartupAction(Binder binder,
      Class<? extends ExceptionalCommand> actionClass) {

    Multibinder.newSetBinder(binder, ExceptionalCommand.class, StartupStage.class)
        .addBinding().to(actionClass);
  }

  /**
   * Startup command to register the shutdown registry as a process shutdown hook.
   */
  private static class ShutdownHookRegistration implements Command {
    private final Command shutdownCommand;

    @Inject ShutdownHookRegistration(@ShutdownStage Command shutdownCommand) {
      this.shutdownCommand = checkNotNull(shutdownCommand);
    }

    @Override public void execute() {
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override public void run() {
          shutdownCommand.execute();
        }
      }, "ShutdownRegistry-Hook"));
    }
  }

  /**
   * Startup command that ensures startup and shutdown of local services.
   */
  private static class LocalServiceLauncher implements Command {
    private final LocalServiceRegistry serviceRegistry;

    @Inject LocalServiceLauncher(LocalServiceRegistry serviceRegistry) {
      this.serviceRegistry = checkNotNull(serviceRegistry);
    }

    @Override public void execute() {
      serviceRegistry.ensureLaunched();
    }
  }
}
