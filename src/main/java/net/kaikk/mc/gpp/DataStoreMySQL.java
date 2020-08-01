/*
GriefPreventionPlus Server Plugin for Minecraft
Copyright (C) 2015 Antonino Kai Pocorobba
(forked from GriefPrevention by Ryan Hamshire)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package net.kaikk.mc.gpp;

import org.bukkit.World;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

//singleton class which manages all GriefPrevention data (except for config options)
public class DataStoreMySQL extends DataStore{

    private Connection databaseConnection = null;
    private final String databaseUrl, userName, password;

    DataStoreMySQL(String url, String userName, String password){
        super();
        this.databaseUrl = url;
        this.userName = userName;
        this.password = password;
    }

    // initialization!
    @Override
    void initialize() throws Exception {
        try {
            // load the java driver for mySQL
            Class.forName("com.mysql.jdbc.Driver");
        } catch (final Exception e) {
            GriefPreventionPlus.addLogEntry("ERROR: Unable to load Java's mySQL database driver.  Check to make sure you've installed it properly.");
            throw e;
        }

        try {
            this.refreshDataConnection();
        } catch (final Exception e2) {
            GriefPreventionPlus.addLogEntry("ERROR: Unable to connect to database.  Check your config file settings.");
            throw e2;
        }

        try {
            final Statement statement = this.databaseConnection.createStatement();

            ResultSet results = statement.executeQuery("SHOW TABLES LIKE 'gpp_claims'");
            if (!results.next()) {
                statement.execute("CREATE TABLE IF NOT EXISTS gpp_claims (id int(11) NOT NULL AUTO_INCREMENT,owner binary(16) NOT NULL COMMENT 'UUID',world binary(16) NOT NULL COMMENT 'UUID',lesserX mediumint(9) NOT NULL,lesserZ mediumint(9) NOT NULL,greaterX mediumint(9) NOT NULL,greaterZ mediumint(9) NOT NULL,parentid int(11),creation bigint(20) NOT NULL,PRIMARY KEY (id));");

                statement.execute("CREATE TABLE IF NOT EXISTS gpp_groupdata (gname varchar(100) NOT NULL,blocks int(11) NOT NULL,UNIQUE KEY gname (gname));");

                statement.execute("CREATE TABLE IF NOT EXISTS gpp_permsbukkit (claimid int(11) NOT NULL,pname varchar(80) NOT NULL,perm tinyint(4) NOT NULL,PRIMARY KEY (claimid,pname),KEY claimid (claimid));");

                statement.execute("CREATE TABLE IF NOT EXISTS gpp_permsplayer (claimid int(11) NOT NULL,player binary(16) NOT NULL COMMENT 'UUID',perm tinyint(4) NOT NULL,PRIMARY KEY (claimid,player),KEY claimid (claimid));");

                statement.execute("CREATE TABLE IF NOT EXISTS gpp_playerdata (player binary(16) NOT NULL COMMENT 'UUID',accruedblocks int(11) NOT NULL,bonusblocks int(11) NOT NULL,lastseen bigint(20) NOT NULL, PRIMARY KEY (player));");

                results = statement.executeQuery("SHOW TABLES LIKE 'griefprevention_claimdata';");
                if (results.next()) {
                    // migration from griefprevention
                    GriefPreventionPlus.addLogEntry("Migrating data from Grief Prevention. It may take some time.");

                    // claims
                    results = statement.executeQuery("SELECT * FROM griefprevention_claimdata ORDER BY parentid ASC;");
                    final Statement statement2 = this.databaseConnection.createStatement();

                    String tString;
                    String playerId;
                    long i = 0;
                    long j = 0;
                    long k = 0;

                    long claimId = 1;
                    Long nextParentId;

                    final HashMap<Long, Long> migratedClaims = new HashMap<Long, Long>();
                    while (results.next()) {
                        final String ownerString = results.getString(2);
                        playerId = "0";

                        if ((ownerString.length() == 36) && ((tString = ownerString.replace("-", "")).length() == 32)) {
                            playerId = "0x" + tString;
                        }

                        final String[] lesser = results.getString(3).split(";");
                        final String[] greater = results.getString(4).split(";");
                        if ((lesser.length != 4) || (greater.length != 4)) { // wrong
                            // corners,
                            // skip
                            // this
                            // claim
                            GriefPreventionPlus.addLogEntry("Skipping claim " + results.getLong(1) + ": wrong corners");
                            continue;
                        }

                        final World world = GriefPreventionPlus.getInstance().getServer().getWorld(lesser[0]);
                        if (world == null) { // this world doesn't exist, skip
                            // this claim
                            GriefPreventionPlus.addLogEntry("Skipping claim " + results.getLong(1) + ": world " + lesser[0] + " doesn't exist");
                            continue;
                        }

                        // insert this claim in new claims table

                        if (results.getLong(9) == -1) { // claims
                            migratedClaims.put(results.getLong(1), claimId++);
                            nextParentId = (long) -1;
                            if (playerId.equals("0")) {
                                playerId = UUIDtoHexString(GriefPreventionPlus.UUID1); // administrative
                                // claims
                            }
                        } else { // subclaims
                            nextParentId = migratedClaims.get(results.getLong(9));
                        }

                        if (nextParentId == null) {
                            GriefPreventionPlus.addLogEntry("Skipping orphan subclaim (parentid: " + results.getLong(9) + ").");
                            continue;
                        }

                        statement2.executeUpdate("INSERT INTO gpp_claims (owner, world, lesserX, lesserZ, greaterX, greaterZ, parentid, creation) VALUES (" + playerId + ", " + UUIDtoHexString(world.getUID()) + ", " + lesser[1] + ", " + lesser[3] + ", " + greater[1] + ", " + greater[3] + ", " + nextParentId + ", 0);");

                        i++;

                        // convert permissions for this claim
                        // builders
                        if (!results.getString(5).isEmpty()) {
                            for (final String s : results.getString(5).split(";")) {
                                if (s.startsWith("[")) {
                                    statement2.executeUpdate("INSERT INTO gpp_permsbukkit VALUES(" + i + ", '" + s.substring(1, s.length() - 1) + "', 2) ON DUPLICATE KEY UPDATE perm = perm | 2;");
                                } else {
                                    if ((s.length() == 36) && ((tString = s.replace("-", "")).length() == 32)) {
                                        statement2.executeUpdate("INSERT INTO gpp_permsplayer VALUES(" + i + ", 0x" + tString + ", 2) ON DUPLICATE KEY UPDATE perm = perm | 2;");
                                    } else if (s.equals("public")) {
                                        statement2.executeUpdate("INSERT INTO gpp_permsplayer VALUES(" + i + ", " + UUIDtoHexString(GriefPreventionPlus.UUID0) + ", 2) ON DUPLICATE KEY UPDATE perm = perm | 2;");
                                    }
                                }
                                j++;
                            }
                        }

                        // containers
                        if (!results.getString(6).isEmpty()) {
                            for (final String s : results.getString(6).split(";")) {
                                if (s.startsWith("[")) {
                                    statement2.executeUpdate("INSERT INTO gpp_permsbukkit VALUES(" + i + ", '" + s.substring(1, s.length() - 1) + "', 4) ON DUPLICATE KEY UPDATE perm = perm | 4;");
                                } else {
                                    if ((s.length() == 36) && ((tString = s.replace("-", "")).length() == 32)) {
                                        statement2.executeUpdate("INSERT INTO gpp_permsplayer VALUES(" + i + ", 0x" + tString + ", 4) ON DUPLICATE KEY UPDATE perm = perm | 4;");
                                    } else if (s.equals("public")) {
                                        statement2.executeUpdate("INSERT INTO gpp_permsplayer VALUES(" + i + ", " + UUIDtoHexString(GriefPreventionPlus.UUID0) + ", 4) ON DUPLICATE KEY UPDATE perm = perm | 4;");
                                    }
                                }
                                j++;
                            }
                        }

                        // accessors
                        if (!results.getString(7).isEmpty()) {
                            for (final String s : results.getString(7).split(";")) {
                                if (s.startsWith("[")) {
                                    statement2.executeUpdate("INSERT INTO gpp_permsbukkit VALUES(" + i + ", '" + s.substring(1, s.length() - 1) + "', 8) ON DUPLICATE KEY UPDATE perm = perm | 8;");
                                } else {
                                    if ((s.length() == 36) && ((tString = s.replace("-", "")).length() == 32)) {
                                        statement2.executeUpdate("INSERT INTO gpp_permsplayer VALUES(" + i + ", 0x" + tString + ", 8) ON DUPLICATE KEY UPDATE perm = perm | 8;");
                                    } else if (s.equals("public")) {
                                        statement2.executeUpdate("INSERT INTO gpp_permsplayer VALUES(" + i + ", " + UUIDtoHexString(GriefPreventionPlus.UUID0) + ", 8) ON DUPLICATE KEY UPDATE perm = perm | 8;");
                                    }
                                }
                                j++;
                            }
                        }

                        // managers
                        if (!results.getString(8).isEmpty()) {
                            for (final String s : results.getString(8).split(";")) {
                                if (s.startsWith("[")) {
                                    statement2.executeUpdate("INSERT INTO gpp_permsbukkit VALUES(" + i + ", '" + s.substring(1, s.length() - 1) + "', 1) ON DUPLICATE KEY UPDATE perm = perm | 1;");
                                } else {
                                    if ((s.length() == 36) && ((tString = s.replace("-", "")).length() == 32)) {
                                        statement2.executeUpdate("INSERT INTO gpp_permsplayer VALUES(" + i + ", 0x" + tString + ", 1) ON DUPLICATE KEY UPDATE perm = perm | 1;");
                                    } else if (s.equals("public")) {
                                        statement2.executeUpdate("INSERT INTO gpp_permsplayer VALUES(" + i + ", " + UUIDtoHexString(GriefPreventionPlus.UUID0) + ", 1) ON DUPLICATE KEY UPDATE perm = perm | 1;");
                                    }
                                }
                                j++;
                            }
                        }
                    }

                    results = statement.executeQuery("SELECT name, accruedblocks, bonusblocks FROM griefprevention_playerdata;");

                    final Map<String, Integer[]> claimBlocksMap = new HashMap<String, Integer[]>();
                    while (results.next()) {
                        final String ownerString = results.getString(1);

                        if ((ownerString.length() == 36) && ((tString = ownerString.replace("-", "")).length() == 32)) {
                            final Integer[] existingBlocks = claimBlocksMap.get(tString);
                            if (existingBlocks != null) {
                                GriefPreventionPlus.addLogEntry("WARNING: Found duplicated key for " + tString);

                                final int a = existingBlocks[0];
                                final int b = existingBlocks[1];

                                final Integer[] blocks = { (results.getInt(2) == a ? a : results.getInt(2) + a), (results.getInt(3) == b ? b : results.getInt(3) + b) };
                                claimBlocksMap.put(tString, blocks);
                            } else {
                                final Integer[] blocks = { results.getInt(2), results.getInt(3) };
                                claimBlocksMap.put(tString, blocks);
                            }

                            playerId = tString;
                        } else {
                            GriefPreventionPlus.addLogEntry("Skipping GriefPrevention data for user " + ownerString + ": no UUID.");
                            continue;
                        }
                    }

                    for (final Entry<String, Integer[]> gppbf : claimBlocksMap.entrySet()) {
                        statement2.executeUpdate("INSERT INTO gpp_playerdata VALUES (0x" + gppbf.getKey() + ", " + gppbf.getValue()[0] + ", " + gppbf.getValue()[1] + ", 0);");
                        k++;
                    }

                    statement.close();
                    statement2.close();
                    GriefPreventionPlus.addLogEntry("Migration complete. Claims: " + i + " - Permissions: " + j + " - PlayerData: " + k);
                }
            } else {
                // database updates

                // v13.0 - added lastseen column to the playerdata table
                Statement s = this.databaseConnection.createStatement();
                ResultSet rs = s.executeQuery("SHOW COLUMNS FROM gpp_playerdata LIKE 'lastseen';");
                if (!rs.next()) {
                    s.executeUpdate("ALTER TABLE gpp_playerdata ADD lastseen BIGINT NOT NULL DEFAULT '0' AFTER bonusblocks;");
                }
                // v13.0 - added creation date column to the claims table
                rs = s.executeQuery("SHOW COLUMNS FROM gpp_claims LIKE 'creation';");
                if (!rs.next()) {
                    s.executeUpdate("ALTER TABLE gpp_claims ADD creation BIGINT NOT NULL DEFAULT '0' AFTER parentid;");
                }
            }
        } catch (final Exception e3) {
            GriefPreventionPlus.addLogEntry("ERROR: Unable to create the necessary database table.  Details:");
            GriefPreventionPlus.addLogEntry(e3.getMessage());
            e3.printStackTrace();
            throw e3;
        }

        // load group data into memory
        final Statement statement = this.databaseConnection.createStatement();
        ResultSet results = statement.executeQuery("SELECT gname, blocks FROM gpp_groupdata;");

        while (results.next()) {
            this.permissionToBonusBlocksMap.put(results.getString(1), results.getInt(2));
        }

        // load claims data into memory
        results = statement.executeQuery("SELECT * FROM gpp_claims;");
        final Statement statementPerms = this.databaseConnection.createStatement();
        ResultSet resultsPerms;

        while (results.next()) {
            final int id = results.getInt(1);
            final int parentid = results.getInt(8);
            UUID owner = null;
            final HashMap<UUID, Integer> permissionMapPlayers = new HashMap<UUID, Integer>();
            final HashMap<String, Integer> permissionMapBukkit = new HashMap<String, Integer>();
            final HashMap<String, Integer> permissionMapFakePlayer = new HashMap<String, Integer>();

            final UUID world = toUUID(results.getBytes(3));

            if (results.getBytes(2) != null) {
                owner = toUUID(results.getBytes(2));
            }

            resultsPerms = statementPerms.executeQuery("SELECT player, perm FROM gpp_permsplayer WHERE claimid=" + id + ";");
            while (resultsPerms.next()) {
                permissionMapPlayers.put(toUUID(resultsPerms.getBytes(1)), resultsPerms.getInt(2));
            }

            resultsPerms = statementPerms.executeQuery("SELECT pname, perm FROM gpp_permsbukkit WHERE claimid=" + id + ";");
            while (resultsPerms.next()) {
                if (resultsPerms.getString(1).startsWith("#")) {
                    permissionMapFakePlayer.put(resultsPerms.getString(1).substring(1), resultsPerms.getInt(2));
                } else {
                    permissionMapBukkit.put(resultsPerms.getString(1), resultsPerms.getInt(2));
                }
            }

            final Claim claim = new Claim(world, results.getInt(4), results.getInt(5), results.getInt(6), results.getInt(7), owner, permissionMapPlayers, permissionMapBukkit, permissionMapFakePlayer, id, results.getLong(9));

            if (parentid == -1) {
                this.addClaim(claim, false);
            } else {
                final Claim topClaim = this.claims.get(parentid);
                if (topClaim == null) {
                    // parent claim doesn't exist, skip this subclaim
                    GriefPreventionPlus.addLogEntry("Orphan subclaim: " + claim.locationToString());
                    continue;
                }
                claim.setParent(topClaim);
                topClaim.getChildren().add(claim);
            }
        }

        GriefPreventionPlus.addLogEntry(this.claims.size() + " total claims loaded.");

        cachePlayersData();
        GriefPreventionPlus.addLogEntry("Cached "+ this.playersData.size() + " players.");

        // load up all the messages from messages.yml
        this.loadMessages();
        GriefPreventionPlus.addLogEntry("Customizable messages loaded.");

        // try to hook into world guard
        try {
            this.worldGuard = new WorldGuardWrapper();
            GriefPreventionPlus.addLogEntry("Successfully hooked into WorldGuard.");
        }
        // if failed, world guard compat features will just be disabled.
        catch (final ClassNotFoundException exception) {
        } catch (final NoClassDefFoundError exception) {
        }
    }

    /** saves changes to player data. MUST be called after you're done making
     changes, otherwise a reload will lose them */
    @Override
    public void asyncSavePlayerData(UUID playerID, PlayerData playerData) {
        // never save data for the "administrative" account. an empty string for
        // player name indicates administrative account
        if (playerID == null) {
            return;
        }

        try {
            this.refreshDataConnection();

            final Statement statement = this.databaseConnection.createStatement();
            statement.executeUpdate("INSERT INTO gpp_playerdata VALUES (" + UUIDtoHexString(playerData.playerID) + ", \"" + playerData.getAccruedClaimBlocks() + "\", " + playerData.getBonusClaimBlocks() + ", "+playerData.lastSeen+") ON DUPLICATE KEY UPDATE accruedblocks=" + playerData.getAccruedClaimBlocks() + ", bonusblocks=" + playerData.getBonusClaimBlocks() + ", lastseen = "+playerData.lastSeen+";");
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to save data for player " + playerID.toString() + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** saves changes to player data to secondary storage. MUST be called after
     you're done making changes, otherwise a reload will lose them */
    @Override
    public void savePlayerData(UUID playerID, PlayerData playerData) {
        new SavePlayerDataThread(playerID, playerData).start();
    }

    /** saves changes to player data to secondary storage. MUST be called after
     you're done making changes, otherwise a reload will lose them */
    @Override
    public void savePlayerDataSync(UUID playerID, PlayerData playerData) {
        // ensure player data is already read from file before trying to save
        playerData.getAccruedClaimBlocks();
        playerData.getClaims();

        //TODO Create a way to save MySQL Sync, like, in another world!
        this.asyncSavePlayerData(playerID, playerData);
    }

    @Override
    int clearOrphanClaims() {
        int count = 0;
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();
            final Statement statement2 = this.databaseConnection.createStatement();
            final ResultSet results = statement.executeQuery("SELECT * FROM gpp_claims;");

            while (results.next()) {
                final World world = GriefPreventionPlus.getInstance().getServer().getWorld(toUUID(results.getBytes(3)));
                if ((world == null) || ((results.getInt(8) != -1) && (this.getClaim(results.getInt(8)) == null))) {
                    statement2.executeUpdate("DELETE FROM gpp_claims WHERE id=" + results.getInt(1));
                    count++;
                }
            }
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("SQL Error during clear orphan claims. Details: " + e.getMessage());
        }

        return count;
    }

    @Override
    void close() {
        if (this.databaseConnection != null) {
            try {
                if (!this.databaseConnection.isClosed()) {
                    this.databaseConnection.close();
                }
            } catch (final SQLException e) {
            }
            ;
        }

        this.databaseConnection = null;
    }

    @Override
    void dbNewClaim(Claim claim) {
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();

            statement.executeUpdate("INSERT INTO gpp_claims (owner, world, lesserX, lesserZ, greaterX, greaterZ, parentid, creation) VALUES (" + UUIDtoHexString(claim.getOwnerID()) + ", " + UUIDtoHexString(claim.getLesserBoundaryCorner().getWorld().getUID()) + ", " + claim.getLesserBoundaryCorner().getBlockX() + ", " + claim.getLesserBoundaryCorner().getBlockZ() + ", " + claim.getGreaterBoundaryCorner().getBlockX() + ", " + claim.getGreaterBoundaryCorner().getBlockZ() + ", " + (claim.getParent() != null ? claim.getParent().id : -1) + ", " + claim.getCreationDate() + ");", Statement.RETURN_GENERATED_KEYS);

            final ResultSet result = statement.getGeneratedKeys();
            result.next();
            claim.id = result.getInt(1);
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to insert data for new claim at " + claim.locationToString() + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    @Override
    void dbSetPerm(Integer claimId, String permString, int perm) {
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();

            statement.executeUpdate("INSERT INTO gpp_permsbukkit VALUES (" + claimId + ", \"" + permString + "\", " + perm + ") ON DUPLICATE KEY UPDATE perm=perm | " + perm + ";");
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to set perms for claim id " + claimId + " perm [" + permString + "].  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    @Override
    void dbSetPerm(Integer claimId, UUID playerId, int perm) {
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();

            statement.executeUpdate("INSERT INTO gpp_permsplayer VALUES (" + claimId + ", " + UUIDtoHexString(playerId) + ", " + perm + ") ON DUPLICATE KEY UPDATE perm=perm | " + perm + ";");
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to set perms for claim id " + claimId + " player {" + playerId.toString() + "}.  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset all claim's perms */
    @Override
    void dbUnsetPerm(Integer claimId) {
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();

            statement.executeUpdate("DELETE FROM gpp_permsplayer WHERE claimid=" + claimId + ";");
            statement.executeUpdate("DELETE FROM gpp_permsbukkit WHERE claimid=" + claimId + ";");
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to unset perms for claim id " + claimId + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset permBukkit's perm from claim */
    @Override
    void dbUnsetPerm(Integer claimId, String permString) {
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();

            statement.executeUpdate("DELETE FROM gpp_permsbukkit WHERE claimid=" + claimId + " AND pname=\"" + permString + "\";");
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to unset perms for claim id " + claimId + " perm [" + permString + "].  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset playerId's perm from claim */
    @Override
    void dbUnsetPerm(Integer claimId, UUID playerId) {
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();

            statement.executeUpdate("DELETE FROM gpp_permsplayer WHERE claimid=" + claimId + " AND player=" + UUIDtoHexString(playerId) + ";");
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to unset perms for claim id " + claimId + " player {" + playerId.toString() + "}.  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset all player claims' perms */
    @Override
    void dbUnsetPerm(UUID playerId) {
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();

            statement.executeUpdate("DELETE p FROM gpp_permsplayer AS p INNER JOIN gpp_claims AS c ON p.claimid = c.id WHERE c.owner=" + UUIDtoHexString(playerId) + ";");
            statement.executeUpdate("DELETE p FROM gpp_permsbukkit AS p INNER JOIN gpp_claims AS c ON p.claimid = c.id WHERE c.owner=" + UUIDtoHexString(playerId) + ";");
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to unset perms for " + playerId.toString() + "'s claims.  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset permbukkit perms from all owner's claim */
    @Override
    void dbUnsetPerm(UUID owner, String permString) {
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();

            statement.executeUpdate("DELETE p FROM gpp_permsbukkit AS p INNER JOIN gpp_claims AS c ON p.claimid = c.id WHERE c.owner=" + UUIDtoHexString(owner) + " AND p.pname=\"" + permString + "\";");
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to unset [" + permString + "] perms from {" + owner.toString() + "}'s claims.  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset playerId perms from all owner's claim */
    @Override
    void dbUnsetPerm(UUID owner, UUID playerId) {
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();

            statement.executeUpdate("DELETE p FROM gpp_permsplayer AS p INNER JOIN gpp_claims AS c ON p.claimid = c.id WHERE c.owner=" + UUIDtoHexString(owner) + " AND p.player=" + UUIDtoHexString(playerId) + ";");
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to unset {" + playerId.toString() + "} perms from {" + owner.toString() + "}'s claims.  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    @Override
    void dbUpdateLocation(Claim claim) {
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();

            statement.executeUpdate("UPDATE gpp_claims SET lesserX=" + claim.getLesserBoundaryCorner().getBlockX() + ", lesserZ=" + claim.getLesserBoundaryCorner().getBlockZ() + ", greaterX=" + claim.getGreaterBoundaryCorner().getBlockX() + ", greaterZ=" + claim.getGreaterBoundaryCorner().getBlockZ() + " WHERE id=" + claim.id);
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to update location for claim id " + claim.id + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    @Override
    void dbUpdateOwner(Claim claim) {
        try {
            this.refreshDataConnection();
            final Statement statement = this.databaseConnection.createStatement();

            statement.executeUpdate("UPDATE gpp_claims SET owner=" + UUIDtoHexString(claim.getOwnerID()) + " WHERE id=" + claim.id);
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to update owner for claim id " + claim.id + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    // deletes a claim from the database (this delete subclaims too)
    @Override
    void deleteClaimFromSecondaryStorage(Claim claim) {
        try {
            this.refreshDataConnection();

            final Statement statement = this.databaseConnection.createStatement();
            if (claim.getChildren().isEmpty()) {
                statement.execute("DELETE p FROM gpp_claims AS c RIGHT JOIN gpp_permsbukkit AS p ON c.id = p.claimid WHERE c.id=" + claim.id + ";");
                statement.execute("DELETE p FROM gpp_claims AS c RIGHT JOIN gpp_permsplayer AS p ON c.id = p.claimid WHERE c.id=" + claim.id + ";");
                statement.execute("DELETE FROM gpp_claims WHERE id=" + claim.id + ";");
            } else {
                statement.execute("DELETE p FROM gpp_claims AS c RIGHT JOIN gpp_permsbukkit AS p ON c.id = p.claimid WHERE c.id=" + claim.id + " OR c.parentid=" + claim.id + ";");
                statement.execute("DELETE p FROM gpp_claims AS c RIGHT JOIN gpp_permsplayer AS p ON c.id = p.claimid WHERE c.id=" + claim.id + " OR c.parentid=" + claim.id + ";");
                statement.execute("DELETE FROM gpp_claims WHERE id=" + claim.id + " OR parentid=" + claim.id + ";");
            }
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to delete data for claim " + claim.id + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    PlayerData getPlayerDataFromStorage(UUID playerID) {
        try {
            this.refreshDataConnection();

            final Statement statement = this.databaseConnection.createStatement();
            final ResultSet results = statement.executeQuery("SELECT * FROM gpp_playerdata WHERE player=" + UUIDtoHexString(playerID) + ";");

            // if data for this player exists, use it
            if (results.next()) {
                return new PlayerData(playerID, results.getInt(2), results.getInt(3), results.getLong(4));
            }
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to retrieve data for player " + playerID.toString() + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
            e.printStackTrace();
        }

        return null;
    }

    @Override
    void cachePlayersData() {
        try {
            this.refreshDataConnection();

            final Statement statement = this.databaseConnection.createStatement();
            final ResultSet results = statement.executeQuery("SELECT * FROM gpp_playerdata WHERE lastseen > "+(System.currentTimeMillis()-(60*60*24*30*1000L))+";");

            // if data for this player exists, use it
            while(results.next()) {
                UUID uuid = toUUID(results.getBytes(1));
                this.playersData.put(uuid, new PlayerData(uuid, results.getInt(2), results.getInt(3), results.getLong(4)));
            }
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to cache players data. Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
            e.printStackTrace();
        }
    }

    List<PlayerData> getEntirePlayerDataFromDatabase() {
        try {
            this.refreshDataConnection();

            final Statement statement = this.databaseConnection.createStatement();
            final ResultSet results = statement.executeQuery("SELECT * FROM gpp_playerdata;");

            // if data for this player exists, use it
            List<PlayerData> playerDataList = new ArrayList<>();
            while(results.next()) {
                UUID uuid = toUUID(results.getBytes(1));
                playerDataList.add(new PlayerData(uuid, results.getInt(2), results.getInt(3), results.getLong(4)));
            }
            return playerDataList;
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to cache players data. Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to cache playerdata manito!");
        }
    }

    void refreshDataConnection() throws SQLException {
        if ((this.databaseConnection == null) || this.databaseConnection.isClosed()) {
            // set username/pass properties
            final Properties connectionProps = new Properties();
            connectionProps.put("user", this.userName);
            connectionProps.put("password", this.password);
            connectionProps.put("autoReconnect", "true");
            connectionProps.put("maxReconnects", "4");

            // establish connection
            this.databaseConnection = DriverManager.getConnection(this.databaseUrl, connectionProps);
        }
    }

    // updates the database with a group's bonus blocks
    @Override
    void saveGroupBonusBlocks(String groupName, int currentValue) {
        // group bonus blocks are stored in the player data table, with player
        // name = $groupName
        try {
            this.refreshDataConnection();

            final Statement statement = this.databaseConnection.createStatement();
            statement.executeUpdate("INSERT INTO gpp_groupdata VALUES (\"" + groupName + "\", " + currentValue + ") ON DUPLICATE KEY UPDATE blocks=" + currentValue + ";");
        } catch (final SQLException e) {
            GriefPreventionPlus.addLogEntry("Unable to save data for group " + groupName + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    private class SavePlayerDataThread extends Thread {
        private final UUID playerID;
        private final PlayerData playerData;

        SavePlayerDataThread(UUID playerID, PlayerData playerData) {
            this.playerID = playerID;
            this.playerData = playerData;
        }

        @Override
        public void run() {
            // ensure player data is already read from file before trying to save
            this.playerData.getAccruedClaimBlocks();
            this.playerData.getClaims();
            DataStoreMySQL.this.asyncSavePlayerData(this.playerID, this.playerData);
        }
    }
}
