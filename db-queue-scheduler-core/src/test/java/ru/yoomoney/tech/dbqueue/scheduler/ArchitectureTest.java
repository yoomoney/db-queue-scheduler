package ru.yoomoney.tech.dbqueue.scheduler;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public class ArchitectureTest {
    private static final String BASE_PACKAGE = "ru.yoomoney.tech.dbqueue.scheduler";

    private JavaClasses importedClasses;

    @BeforeEach
    public void importClasses() {
        this.importedClasses = new ClassFileImporter().importPackages(BASE_PACKAGE);
    }

    @Test
    public void public_classes_must_not_depends_on_internal() {
        ArchRule rule = noClasses()
                .that()
                    .resideOutsideOfPackage("ru.yoomoney.tech.dbqueue.scheduler.internal..")
                    .and()
                    .arePublic()
                .should()
                    .accessClassesThat().resideInAnyPackage("ru.yoomoney.tech.dbqueue.scheduler.internal..")
                .because("public classes must not depend on internal details");
        rule.check(importedClasses);
    }
}
