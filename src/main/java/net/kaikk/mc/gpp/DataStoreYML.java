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

import br.com.finalcraft.gpp.config.FCConfig;
import org.bukkit.Bukkit;
import org.bukkit.World;
import org.bukkit.configuration.file.FileConfiguration;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

//singleton class which manages all GriefPrevention data (except for config options)
public class DataStoreYML extends DataStore{
    DataStoreYML(){
        super();
    }

    public Map<Integer, FileConfiguration> claimsCfgFiles = new HashMap<>();

    private FCConfig gpp_groupdata;
    private HashMap<Integer, FCConfig> gpp_claims = new HashMap<>();
    private HashMap<UUID, FCConfig> gpp_playerdata = new HashMap<>();

    private final File gppDataFolder = new File(new File(DataStore.configFilePath).getParent(), "DataStorageYML");
    private final File claimDataFolder = new File(gppDataFolder, "claims");
    private final File playersDataFolder = new File(gppDataFolder, "players");


    // initialization!
    @Override
    void initialize() throws Exception {

        gppDataFolder.mkdirs();
        claimDataFolder.mkdirs();

        gpp_groupdata = new FCConfig(new File(gppDataFolder, "gpp_groupdata.yml"));

        if (playersDataFolder.listFiles() != null){
            for (File file : playersDataFolder.listFiles()) {
                if (file.getName().endsWith(".yml")){
                    try {
                        UUID player_uuid = UUID.fromString(file.getName().substring(0, file.getName().length() - 4));
                        final FCConfig fcconfig = new FCConfig(file);
                        gpp_playerdata.put(player_uuid, fcconfig);
                    }catch (Exception e){
                        GriefPreventionPlus.addLogEntry("-- WARNING  --");
                        GriefPreventionPlus.addLogEntry("I cant read the PlayerData info from " + file.getPath());
                        e.printStackTrace();
                        GriefPreventionPlus.addLogEntry("--------------");
                    }

                }
            }
        }

        if (claimDataFolder.listFiles() != null){
            for (File file : claimDataFolder.listFiles()) {
                try {
                    if (file.getName().endsWith(".yml")){
                        Integer claim_id = Integer.parseInt(file.getName().split(Pattern.quote("."))[0]);

                        final FCConfig fcconfig = new FCConfig(file);

                        final UUID owner = fcconfig.getUUID("ClaimData.owner");
                        final UUID world = fcconfig.getUUID("ClaimData.world");
                        final int lesserX = fcconfig.getInt("ClaimData.lesserX");
                        final int lesserZ = fcconfig.getInt("ClaimData.lesserZ");
                        final int greaterX = fcconfig.getInt("ClaimData.greaterX");
                        final int greaterZ = fcconfig.getInt("ClaimData.greaterZ");
                        final int parentid = fcconfig.getInt("ClaimData.parentid");
                        final long creation = fcconfig.getLong("ClaimData.creation");

                        final HashMap<UUID, Integer> permissionMapPlayers = new HashMap<UUID, Integer>();
                        final HashMap<String, Integer> permissionMapBukkit = new HashMap<String, Integer>();
                        final HashMap<String, Integer> permissionMapFakePlayer = new HashMap<String, Integer>();

                        for (String player_uuid : fcconfig.getKeys("ClaimData.playerPerms")) {
                            int perm_level = fcconfig.getInt("ClaimData.playerPerms." + player_uuid);
                            permissionMapPlayers.put(UUID.fromString(player_uuid), perm_level);
                        }

                        for (String perm_name : fcconfig.getKeys("ClaimData.bukkitPerms")) {
                            int perm_level = fcconfig.getInt("ClaimData.bukkitPerms." + perm_name);
                            perm_name = perm_name.replace("§",".");//Fix cases where the BukkitPerm has dots
                            if (perm_name.startsWith("#")) {
                                permissionMapFakePlayer.put(perm_name.substring(1), perm_level);
                            } else {
                                permissionMapBukkit.put(perm_name, perm_level);
                            }
                        }

                        final Claim claim = new Claim(world, lesserX, lesserZ, greaterX, greaterZ, owner, permissionMapPlayers, permissionMapBukkit, permissionMapFakePlayer, claim_id, creation);

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

                        gpp_claims.put(claim_id, fcconfig);
                    }
                }catch (Exception e){
                    GriefPreventionPlus.addLogEntry("-- WARNING  --");
                    GriefPreventionPlus.addLogEntry("I cant read the Claim info from " + file.getPath());
                    e.printStackTrace();
                    GriefPreventionPlus.addLogEntry("--------------");
                }
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
            FCConfig config = gpp_playerdata.get(playerID);
            if (config == null){
                File file = new File(playersDataFolder, playerID.toString() + ".yml");
                if (file.exists()){
                    throw new FileAlreadyExistsException(file.getPath());
                }
                config = new FCConfig(file);
                gpp_playerdata.put(playerID, config);
            }
            config.setValue("GppPlayerData.accruedblocks", playerData.getAccruedClaimBlocks());
            config.setValue("GppPlayerData.bonusblocks", playerData.getBonusClaimBlocks());
            config.setValue("GppPlayerData.lastseen", playerData.lastSeen);
            config.saveAsync();
        } catch (Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to save data for player " + playerID.toString() + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
            e.printStackTrace();
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

        //No need to save this Sync, close() will ensure this is properly executed!
        this.asyncSavePlayerData(playerID, playerData);
    }

    @Override
    int clearOrphanClaims() {
        List<Integer> delList = new ArrayList<>();
        for (Entry<Integer, FCConfig> entry : gpp_claims.entrySet()) {
            try {
                FCConfig config = entry.getValue();

                final UUID worldUUID = config.getUUID("ClaimData.owner");
                final int parentid = config.getInt("ClaimData.parentid");

                World world = Bukkit.getWorld(worldUUID);
                if ((world == null) || ((parentid != -1) && (this.getClaim(parentid) == null))) {
                    try {
                        config.getTheFile().delete();
                        delList.add(entry.getKey());
                    }catch (Exception e){
                        GriefPreventionPlus.addLogEntry("Error deleting the file: " + config.getTheFile().getPath());
                        e.printStackTrace();
                    }
                }
            }catch (Exception e){
                GriefPreventionPlus.addLogEntry("Error during clear orphan claims. Details: " + e.getMessage());
                e.printStackTrace();
            }
        }
        int count = delList.size();
        for (Integer key : delList) {
            gpp_claims.remove(key);
        }
        return count;
    }

    @Override
    void close() {
        if (!FCConfig.scheduler.isShutdown() && !FCConfig.scheduler.isTerminated()){
            try {
                FCConfig.scheduler.shutdown();
                boolean success = FCConfig.scheduler.awaitTermination(30, TimeUnit.SECONDS);
                if (!success){
                    GriefPreventionPlus.addLogEntry("Failed to close DataStoreYML, TimeOut of 30 seconds Reached, this is really bad! Terminating all of them now!");
                    FCConfig.scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //Nothing to close :D
    }

    private Integer lastestID = null;
    @Override
    void dbNewClaim(Claim claim) {
        try {
            if (lastestID == null){
                lastestID = claims.keySet().stream().mapToInt(v -> v).max().orElse(-1);
            }

            lastestID++;

            claim.id = lastestID;

            File newClaimFile = new File(claimDataFolder, claim.id + ".yml");
            if (newClaimFile.exists()){
                throw new RuntimeException("There is no claim for id " + claim.id + ", but there is already a file there! (" + newClaimFile.getPath() + ")");
            }

            FCConfig config = new FCConfig(newClaimFile);
            config.setValue("ClaimData.owner", claim.getOwnerID());
            config.setValue("ClaimData.world", claim.getWorldUID());
            config.setValue("ClaimData.lesserX", claim.lesserX);
            config.setValue("ClaimData.lesserZ", claim.lesserZ);
            config.setValue("ClaimData.greaterX", claim.greaterX);
            config.setValue("ClaimData.greaterZ", claim.greaterZ);
            config.setValue("ClaimData.parentid", claim.getParent() == null ? -1 : claim.getParent().getID());
            config.setValue("ClaimData.creation", claim.getCreationDate());
            config.saveAsync();

            gpp_claims.put(claim.id, config);
        } catch (final Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to insert data for new claim at " + claim.locationToString() + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    void dbNewClaimWithID(Claim claim) {
        try {
            File newClaimFile = new File(claimDataFolder, claim.id + ".yml");
            if (newClaimFile.exists()){
                throw new RuntimeException("There is no claim for id " + claim.id + ", but there is already a file there! (" + newClaimFile.getPath() + ")");
            }

            FCConfig config = new FCConfig(newClaimFile);
            config.setValue("ClaimData.owner", claim.getOwnerID());
            config.setValue("ClaimData.world", claim.getWorldUID());
            config.setValue("ClaimData.lesserX", claim.lesserX);
            config.setValue("ClaimData.lesserZ", claim.lesserZ);
            config.setValue("ClaimData.greaterX", claim.greaterX);
            config.setValue("ClaimData.greaterZ", claim.greaterZ);
            config.setValue("ClaimData.parentid", claim.getParent() == null ? -1 : claim.getParent().getID());
            config.setValue("ClaimData.creation", claim.getCreationDate());
            config.saveAsync();

            gpp_claims.put(claim.id, config);
        } catch (final Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to insert data for new claim at " + claim.locationToString() + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    @Override
    void dbSetPerm(Integer claimId, String permString, int perm) {
        try {
            FCConfig config = gpp_claims.get(claimId);
            permString = permString.replace(".","§");//Fix cases where the BukkitPerm has dots
            int newPerm = config.getInt("ClaimData.bukkitPerms." + permString) | perm; //BitWise OR (simple sum)
            config.setValue("ClaimData.bukkitPerms." + permString, newPerm);
            config.saveAsync();
        } catch (final Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to set perms for claim id " + claimId + " perm [" + permString + "].  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    @Override
    void dbSetPerm(Integer claimId, UUID playerId, int perm) {
        try {
            FCConfig config = gpp_claims.get(claimId);
            int newPerm = config.getInt("ClaimData.playerPerms." + playerId) | perm; //BitWise OR (simple sum)
            config.setValue("ClaimData.playerPerms." + playerId, newPerm);
            config.saveAsync();
        } catch (Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to set perms for claim id " + claimId + " player {" + playerId.toString() + "}.  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset all claim's perms */
    @Override
    void dbUnsetPerm(Integer claimId) {
        try {
            FCConfig config = gpp_claims.get(claimId);
            config.setValue("ClaimData.playerPerms", null);
            config.setValue("ClaimData.bukkitPerms", null);
            config.saveAsync();
        } catch (final Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to unset perms for claim id " + claimId + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset permBukkit's perm from claim */
    @Override
    void dbUnsetPerm(Integer claimId, String permString) {
        try {
            FCConfig config = gpp_claims.get(claimId);
            permString = permString.replace(".","§");//Fix cases where the BukkitPerm has dots
            config.setValue("ClaimData.bukkitPerms." + permString, null);
            config.saveAsync();
        } catch (Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to unset perms for claim id " + claimId + " perm [" + permString + "].  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset playerId's perm from claim */
    @Override
    void dbUnsetPerm(Integer claimId, UUID playerId) {
        try {
            FCConfig config = gpp_claims.get(claimId);
            config.setValue("ClaimData.playerPerms." + playerId, null);
            config.saveAsync();
        } catch (Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to unset perms for claim id " + claimId + " player {" + playerId.toString() + "}.  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset all player claims' perms */
    @Override
    void dbUnsetPerm(UUID playerId) {
        try {
            for (final Claim claim : GriefPreventionPlus.getInstance().getDataStore().claims.values()) {
                if (playerId.equals(claim.getOwnerID())) {
                    FCConfig config = gpp_claims.get(claim.getID());
                    config.setValue("ClaimData.playerPerms", null);
                    config.setValue("ClaimData.bukkitPerms", null);
                    config.saveAsync();
                }
            }
        } catch (Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to unset perms for " + playerId.toString() + "'s claims.  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset permbukkit perms from all owner's claim */
    @Override
    void dbUnsetPerm(UUID owner, String permString) {
        try {
            permString = permString.replace(".","§");//Fix cases where the BukkitPerm has dots
            for (final Claim claim : GriefPreventionPlus.getInstance().getDataStore().claims.values()) {
                if (owner.equals(claim.getOwnerID())) {
                    FCConfig config = gpp_claims.get(claim.getID());
                    config.setValue("ClaimData.bukkitPerms." + permString, null);
                    config.saveAsync();
                }
            }
        } catch (Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to unset [" + permString + "] perms from {" + owner.toString() + "}'s claims.  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    /** Unset playerId perms from all owner's claim */
    @Override
    void dbUnsetPerm(UUID owner, UUID playerId) {
        try {
            for (final Claim claim : GriefPreventionPlus.getInstance().getDataStore().claims.values()) {
                if (owner.equals(claim.getOwnerID())) {
                    FCConfig config = gpp_claims.get(claim.getID());
                    config.setValue("ClaimData.playerPerms." + playerId, null);
                    config.saveAsync();
                }
            }
        } catch (Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to unset {" + playerId.toString() + "} perms from {" + owner.toString() + "}'s claims.  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    @Override
    void dbUpdateLocation(Claim claim) {
        try {
            FCConfig config = gpp_claims.get(claim.getID());
            config.setValue("ClaimData.lesserX", claim.lesserX);
            config.setValue("ClaimData.lesserZ", claim.lesserZ);
            config.setValue("ClaimData.greaterX", claim.greaterX);
            config.setValue("ClaimData.greaterZ", claim.greaterZ);
            config.saveAsync();
        } catch (Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to update location for claim id " + claim.id + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    @Override
    void dbUpdateOwner(Claim claim) {
        try {
            FCConfig config = gpp_claims.get(claim.getID());
            config.setValue("ClaimData.owner", claim.getOwnerID());
            config.saveAsync();
        } catch (Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to update owner for claim id " + claim.id + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
        }
    }

    // deletes a claim from the database (this delete subclaims too)
    @Override
    void deleteClaimFromSecondaryStorage(Claim claim) {
        try {
            List<Claim> claimsToBeDeleted = new ArrayList<>();
            claimsToBeDeleted.add(claim);

            if (!claim.getChildren().isEmpty()){
                claimsToBeDeleted.addAll(claim.getChildren());
            }

            for (Claim claimToBeDeleted : claimsToBeDeleted) {
                FCConfig config = gpp_claims.get(claimToBeDeleted.getID());
                config.getTheFile().delete();
                gpp_claims.remove(claim.getID());
            }
        } catch (Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to delete data for claim " + claim.id + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    PlayerData getPlayerDataFromStorage(UUID playerID) {
        try {//On MYSQL this is needed because eventual non commited changes, here we can use the cached version of PlayerData...
            FCConfig config = gpp_playerdata.get(playerID);
            if (config == null){
                return null;
            }
            return playersData.get(playerID);
        } catch (Exception e) {
            GriefPreventionPlus.addLogEntry("Unable to retrieve data for player " + playerID.toString() + ".  Details:");
            GriefPreventionPlus.addLogEntry(e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    @Override
    void cachePlayersData() {
        //long daysAgo = System.currentTimeMillis() - (60*60*24*30*1000L);
        for (Entry<UUID, FCConfig> uuidfcConfigEntry : gpp_playerdata.entrySet()) {
            try {
                final UUID player = uuidfcConfigEntry.getKey();
                final int accruedblocks = uuidfcConfigEntry.getValue().getInt("GppPlayerData.accruedblocks");
                final int bonusblocks = uuidfcConfigEntry.getValue().getInt("GppPlayerData.bonusblocks");
                final long lastseen = uuidfcConfigEntry.getValue().getLong("GppPlayerData.lastseen");
                //if (lastseen > daysAgo ){ // cache it only if lastlogin was 30 days before now
                    this.playersData.put(player, new PlayerData(player, accruedblocks, bonusblocks, lastseen));
                //}
            } catch (Exception e) {
                GriefPreventionPlus.addLogEntry("Unable to load data from player: " + uuidfcConfigEntry.getKey());
                GriefPreventionPlus.addLogEntry(e.getMessage());
                e.printStackTrace();
            }
        }

    }

    // updates the database with a group's bonus blocks
    @Override
    void saveGroupBonusBlocks(String groupName, int currentValue) {
        // group bonus blocks are stored in the player data table, with player
        // name = $groupName
        try {
            gpp_groupdata.setValue("GroupData." + groupName, currentValue);
            gpp_groupdata.saveAsync();
        } catch (Exception e) {
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
            this.setName("DataStoreYML - SavePlayerDataThread");
        }

        @Override
        public void run() {
            // ensure player data is already read from file before trying to save
            this.playerData.getAccruedClaimBlocks();
            this.playerData.getClaims();
            DataStoreYML.this.asyncSavePlayerData(this.playerID, this.playerData);
        }
    }
}
