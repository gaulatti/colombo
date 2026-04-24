package com.gaulatti.colombo.repository;

import com.gaulatti.colombo.model.Tenant;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository for {@link Tenant} entities.
 *
 * <p>Provides standard CRUD operations inherited from {@link JpaRepository}
 * as well as a custom lookup by FTP username for use during authentication.
 */
public interface TenantRepository extends JpaRepository<Tenant, Long> {

    /**
     * Looks up a tenant by its unique FTP username.
     *
     * @param ftpUsername the FTP login name to search for
     * @return an {@link Optional} containing the matching tenant, or empty if none found
     */
    Optional<Tenant> findByFtpUsername(String ftpUsername);
}

